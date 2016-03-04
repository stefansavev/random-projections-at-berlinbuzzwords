package com.stefansavev.randomprojections.implementation

import java.io.File
import com.stefansavev.core.serialization.core.StringSerializer
import com.stefansavev.randomprojections.buffers.IntArrayBuffer
import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer
import com.stefansavev.randomprojections.interface.Index
import com.stefansavev.randomprojections.serialization.{PointSignaturesSerializer, BinaryFileSerializer}
import com.stefansavev.randomprojections.tuning.PerformanceCounters

class IndexImpl(val signatures: PointSignatures, val totalNumPoints: Int, val leaf2PointsDir: Option[(String, Int, Int, Int)], val leaf2Points: Leaf2Points, val id2Label: Array[Int]) extends Index{
  def toFile(file: File): Unit = {
    val ser = new BinaryFileSerializer(file)
    PointSignaturesSerializer.toBinary(ser.stream, signatures)
    if (leaf2Points != null) {
      ser.putInt(0)
      ser.putIntArrays(leaf2Points.starts, leaf2Points.points)
      ser.putInt(leaf2Points.numberOfLeaves())
    }
    else{
      ser.putInt(1)
      StringSerializer.write(ser.stream, leaf2PointsDir.get._1)
      ser.putInt(leaf2PointsDir.get._2)
      ser.putInt(leaf2PointsDir.get._3)
      ser.putInt(leaf2PointsDir.get._4)
    }
    ser.putInt(totalNumPoints)

    ser.putIntArray(id2Label)

    ser.close()
  }

  def fillCountsWithoutWeights(query: Array[Double], settings: SearcherSettings, trainingSet: DataFrameView, bucketIds: Array[Int], pointCounts: Array[Int], maxIndex: Int, countsOfCounts: Array[Int]): Unit = {
    val signatures = this.signatures
    val sigVectors = settings.randomTrees.signatureVecs
    val querySigs = sigVectors.computePointSignatures(query)
    val leafEnd = bucketIds.length
    var i = 0

    while (i < leafEnd) {
      val leaf = bucketIds(i)
      val pointsStart = leaf2Points.starts(leaf)
      val pointsEnd = leaf2Points.starts(leaf + 1)
      PerformanceCounters.touchedPointsDuringSearch(pointsEnd - pointsStart)
      var j = pointsStart
      while (j < pointsEnd) {
        val pointId = leaf2Points.points(j)
        val prevCount = pointCounts(pointId)
        val inc = if (prevCount == 0) {1 + signatures.overlap(querySigs, pointId)} else 1

        if (prevCount < maxIndex){
          countsOfCounts(prevCount) -= 1
          countsOfCounts(prevCount + inc) += 1
        }
        pointCounts(pointId) = prevCount + inc
        j += 1
      }
      i += 1
    }
    countsOfCounts(0) = 0
  }

  def clearCountsWithoutWeights(bucketIds: Array[Int], pointCounts: Array[Int], pruningThreshold: Int, maxIndex: Int, countsOfCounts: Array[Int], buffer: Array[KNN]): Unit = {
    val leafEnd = bucketIds.length
    var i = 0 //leafStart
    while (i < leafEnd) {
      val leaf = bucketIds(i)
      //val bucketScore = bucketScores(i) //not used
      val pointsStart = leaf2Points.starts(leaf)
      val pointsEnd = leaf2Points.starts(leaf + 1)
      var j = pointsStart
      while (j < pointsEnd) {
        val pointId = leaf2Points.points(j)
        val count = pointCounts(pointId)
        if (count >= pruningThreshold) {
          val index = Math.min(count, maxIndex) - 1
          buffer(buffer.length - countsOfCounts(index) - 1) = new KNN(neighborId = pointId, count = count)
          countsOfCounts(index) += 1
        }
        if (count != 0) {
          pointCounts(pointId) = 0 //also serves to avoid processing the same point twice
        }
        j += 1
      }
      i += 1
    }
  }

  def fillCountsWithWeights(bucketIds: Array[Int], bucketScores: Array[Double], pointCounts: Array[Int], pointWeights: Array[Double]): Unit = {
    //bucket scores are useful when you have only a few trees and you go very fine grained
    val leafEnd = bucketIds.length
    var i = 0 //leafStart

    while (i < leafEnd) {
      val leaf = bucketIds(i)
      val bucketScore = bucketScores(i)
      val pointsStart = leaf2Points.starts(leaf)
      val pointsEnd = leaf2Points.starts(leaf + 1)
      PerformanceCounters.touchedPointsDuringSearch(pointsEnd - pointsStart)
      var j = pointsStart
      while (j < pointsEnd) {
        val pointId = leaf2Points.points(j)
        pointCounts(pointId) += 1
        bucketScores(pointId) += bucketScore
        j += 1
      }
      i += 1
    }
  }

  def getPruningThreshold(maxIndex: Int, countsOfCounts: Array[Int], threshold: Int): (Int, Int) = {
    var i = maxIndex
    var sumCounts = countsOfCounts(maxIndex)
    while(i >= 1 && sumCounts < threshold){
      i -= 1
      sumCounts += countsOfCounts(i)
    }
    PerformanceCounters.nonPrunedPoints(sumCounts)
    val pruningThreshold = Math.max(i, 1) //cannot be zero
    (pruningThreshold, sumCounts)
  }

  def partialSumOnCounts(threshold: Int, countsOfCounts: Array[Int]): Unit = {
    var i = threshold
    countsOfCounts(i - 1) = 0
    while(i < countsOfCounts.length){
      countsOfCounts(i) += countsOfCounts(i - 1)
      i += 1
    }
  }

  def fillDistances(query: Array[Double], rescoreExactlyTopK: Int, buffer: Array[KNN], settings: SearcherSettings, trainingSet: DataFrameView): Unit = {
    var i = 0
    while(i < buffer.length){
      val knn = buffer(i)
      val pointId = knn.neighborId
      knn.label = id2Label(pointId)
      knn.dist = trainingSet.cosineForNormalizedData(query, pointId)
      i += 1
    }
    PerformanceCounters.evaluatedPointsDuringSearch(buffer.length)
  }


  def getNearestNeighbors_NoPruning(k: Int, pointName: Int, query: Array[Double], settings: SearcherSettings, searchBucketsResult: SearchBucketsResult, scratchBuffer: NearestNeigbhorQueryScratchBuffer): KNNS = {
    val bucketIds = searchBucketsResult.bucketIndexBuffer.toArray()
    ///val usesWeights = settings.usesPointWeights TODO
    //val bucketScores = if (usesWeights) (searchBucketsResult.bucketScoreBuffer.toArray()) else null TODO
    val pointCounts = scratchBuffer.pointCounts
    val dataset = settings.trainingSet
    val maxIndex = 64*settings.randomTrees.signatureVecs.numSignatures + settings.randomTrees.trees.length //number of trees
    val countsOfCounts = Array.ofDim[Int](maxIndex + 1) //a point can appear in at most 100 trees

    //Stage 1: Fill the counts of the points (in how many trees(buckets) does the point appear)
    fillCountsWithoutWeights(query, settings, dataset, bucketIds, pointCounts, maxIndex, countsOfCounts)

    //Stage 2: extract the counts and clear the memory
    val (pruningThreshold, expectedCounts) = getPruningThreshold(maxIndex, countsOfCounts, settings.pointScoreSettings.topKCandidates)
    PerformanceCounters.pruningThreshold(pruningThreshold)
    partialSumOnCounts(pruningThreshold, countsOfCounts)
    //Clear the counts
    val buffer = Array.ofDim[KNN](expectedCounts)
    clearCountsWithoutWeights(bucketIds, pointCounts, pruningThreshold, maxIndex, countsOfCounts, buffer)

    PerformanceCounters.thresholdedPointsDuringSearch(buffer.size)


    //val sortedArray = java.util.Arrays.sort .sortBy(-_.similarity) //notice doing it by similarity
    PerformanceCounters.sortedPointsDuringSearch(buffer.size)

    fillDistances(query, settings.pointScoreSettings.rescoreExactlyTopK, buffer, settings, dataset)

    java.util.Arrays.sort(buffer, new KNNDistanceComparator())
    //val topK: Array[KVPair] = buffer.take(k)
    //val knns = topK.map({(kv: KVPair) => KNN(k = k, neighborId = kv.key, count = kv.value, label = id2Label(kv.key), dist = kv.value)})

    KNNS(k, pointName, buffer)
  }


  def fillCountsWithoutWeights(query: Array[Double],
                               settings: SearcherSettings,
                               trainingSet: DataFrameView,
                               bucketIds: Array[Int],
                               pointCounts: Array[Int],
                               maxIndex: Int,
                               countsOfCounts: Array[Int],
                               overlapSize: Int, touchedPoints: IntArrayBuffer = null): Unit = {
    val signatures = this.signatures
    val sigVectors = settings.randomTrees.signatureVecs
    val querySigs = sigVectors.computePointSignatures(query, 0, overlapSize)
    val leafEnd = bucketIds.length
    var i = 0

    while (i < leafEnd) {
      val leaf = bucketIds(i)
      val pointsStart = leaf2Points.starts(leaf)
      val pointsEnd = leaf2Points.starts(leaf + 1)
      PerformanceCounters.touchedPointsDuringSearch(pointsEnd - pointsStart)
      var j = pointsStart
      while (j < pointsEnd) {
        val pointId = leaf2Points.points(j)
        val prevCount = pointCounts(pointId)
        var inc = 1

        if (prevCount == 0) {
          //this is when we see a point for the first time
          inc += signatures.overlap(querySigs, pointId, 0, overlapSize)
          if (touchedPoints != null) {
            touchedPoints += pointId
          }
        }

        if (prevCount < maxIndex){
          countsOfCounts(prevCount) -= 1
          countsOfCounts(prevCount + inc) += 1
        }
        pointCounts(pointId) = prevCount + inc
        j += 1
      }
      i += 1
    }
    countsOfCounts(0) = 0
  }

  def getPruningThresholdAfterStdDevAdjustment(maxIndex: Int, countsOfCounts: Array[Int], threshold: Int, numUsedHashFun: Int): (Int, Int) = {
    var i = maxIndex
    var sumCounts = countsOfCounts(maxIndex)
    while(i >= 1 && sumCounts < threshold){
      i -= 1
      sumCounts += countsOfCounts(i)
    }
    //
    PerformanceCounters.nonPrunedPoints(sumCounts)
    //BUG: i THINK BELOW WE MEAN I, not threshold
    val p = threshold.toDouble/numUsedHashFun.toDouble
    val stdDev = Math.sqrt(p*(1.0 - p)/numUsedHashFun.toDouble)
    val normalPercentile = 2.326348 //1.644854 //for 99 percentile 2.326348
    val expDev = Math.min((0.23*numUsedHashFun).toInt, (normalPercentile*stdDev*numUsedHashFun).toInt) //put some guard
    //println("numUsedHashFun/dev" + numUsedHashFun + "   " + expDev)
    val limit = Math.max(i - expDev /*30*/, 1) //30 -- compute the stddev
    while(i > limit){
      i -= 1
      sumCounts += countsOfCounts(i)
    }
    val pruningThreshold = Math.max(i, 1) //cannot be zero //its more like i - 30 where 30 is the stddev
    //val adjustedThreshold = Math.max(1, pruningThreshold - 30) //TODO: compute the std dev.
    //if you do that then you need to decrement i until i =pruning threshold - 30
    (pruningThreshold, sumCounts)
  }

  def adjustWithoutWeights(query: Array[Double],
                           settings: SearcherSettings,
                           //trainingSet: DataFrameView,
                           pointCounts: Array[Int],
                           maxIndex: Int,
                           countsOfCounts: Array[Int],
                           fromOverlapIndex: Int,
                           toOverlapIndex: Int,
                           prevThreshold: Int,
                           prevTouchedPoints: IntArrayBuffer): Unit = {

    val signatures = this.signatures
    val sigVectors = settings.randomTrees.signatureVecs
    val querySigs = sigVectors.computePointSignatures(query, fromOverlapIndex, toOverlapIndex)

    var i = 0

    while(i < prevTouchedPoints.size) {
      val pointId = prevTouchedPoints(i)
      val prevCount = pointCounts(pointId)

      if (prevCount >= prevThreshold) {
        val inc = signatures.overlap(querySigs, pointId, fromOverlapIndex, toOverlapIndex)
        val newCount = prevCount + inc // Math.min(maxIndex, Math.max(1, prevCount - prevThreshold + inc)) //TODO: NEED TO THINK IF THIS IS A PROBLEM
        countsOfCounts(prevCount) -= 1
        countsOfCounts(newCount) += 1
        pointCounts(pointId) = newCount
        i += 1
      }
      else {
        //prune
        //pointId is the point that is pruned
        val replacePnt = prevTouchedPoints.removeLast()
        prevTouchedPoints.set(i, replacePnt)
        //TODO: IS THIS LINE A BUG: pointCounts(pointId) = COUNT OF REPLACED POINT //no it's not a bug
        pointCounts(pointId) = 0 //reset the counter
        countsOfCounts(prevCount) -= 1
      }
    }
    countsOfCounts(0) = 0 //should not have changed
  }

  def clearCountsWithoutWeightsAfterPruning(bucketIds: Array[Int],
                                            pointCounts: Array[Int],
                                            maxIndex: Int,
                                            countsOfCounts: Array[Int],
                                            remainingPoints: IntArrayBuffer,
                                            pruningThreshold: Int,
                                            buffer: Array[KNN]): Unit = {

    var i = 0
    while(i < remainingPoints.size){
      val pointId = remainingPoints(i)
      val count = pointCounts(pointId)
      pointCounts(pointId) = 0 //reset buffer
      if (count >= pruningThreshold) {
        val index = Math.min(count, maxIndex) - 1
        buffer(buffer.length - countsOfCounts(index) - 1) = new KNN(neighborId = pointId, count = count)
        countsOfCounts(index) += 1
      }
      i += 1
    }

    remainingPoints.clear()
  }

  def getNearestNeighbors(k: Int, pointName: Int, query: Array[Double], settings: SearcherSettings, searchBucketsResult: SearchBucketsResult, scratchBuffer: NearestNeigbhorQueryScratchBuffer): KNNS = {
    val bucketIds = searchBucketsResult.bucketIndexBuffer.toArray()
    ///val usesWeights = settings.usesPointWeights TODO
    //val bucketScores = if (usesWeights) (searchBucketsResult.bucketScoreBuffer.toArray()) else null TODO
    val pointCounts = scratchBuffer.pointCounts
    val dataset = settings.trainingSet
    val totalNumSignatures = settings.randomTrees.signatureVecs.numSignatures
    val initialNumSignatures = 2 //2 //totalNumSignatures/2 //Math.min(2, totalNumSignatures)
    val maxIndex = 64*totalNumSignatures /*initialNumSignatures*/ + settings.randomTrees.trees.length //number of trees
    val countsOfCounts = Array.ofDim[Int](maxIndex + 1) //a point can appear in at most 100 trees

    val touchedPoints = new IntArrayBuffer(1000) //TODO: set properly (maxNumber of touched points per query * numTrees * 2)

    //Stage 1: Fill the counts of the points (in how many trees(buckets) does the point appear)
    /*
    query: Array[Double],
                               settings: SearcherSettings,
                               //trainingSet: DataFrameView,
                               pointCounts: Array[Int],
                               maxIndex: Int,
                               countsOfCounts: Array[Int],
                               fromOverlapIndex: Int,
                               toOverlapIndex: Int,
                               overlapSize: Int,
                               prevThreshold: Int,
                               prevTouchedPoints: IntArrayBuffer
     */

    fillCountsWithoutWeights(query, settings, dataset, bucketIds, pointCounts, maxIndex, countsOfCounts, initialNumSignatures, touchedPoints)
    //Stage 2: extract the counts and clear the memory
    //var variableTopK = 16*settings.pointScoreSettings.topKCandidates
    val variableTopK = settings.pointScoreSettings.topKCandidates
    val r = getPruningThresholdAfterStdDevAdjustment(maxIndex, countsOfCounts, variableTopK, 64*initialNumSignatures)
    var pruningThreshold = r._1
    var expectedCounts = r._2

    var z = initialNumSignatures

    while(z < totalNumSignatures && expectedCounts > settings.pointScoreSettings.topKCandidates){
      //println("Pruning threshold " + z + " " + pruningThreshold + " counts " + expectedCounts)
      //println("z-expcounts " + z + " " + expectedCounts)
      adjustWithoutWeights(query, settings, pointCounts, maxIndex, countsOfCounts, z, z+initialNumSignatures, pruningThreshold, touchedPoints)
      z += initialNumSignatures

      val r = getPruningThresholdAfterStdDevAdjustment(maxIndex, countsOfCounts, variableTopK, 64*z)

      /*
      variableTopK /= 2
      if (variableTopK < settings.pointScoreSettings.topKCandidates){
        variableTopK = settings.pointScoreSettings.topKCandidates
      }
      */
      pruningThreshold = r._1
      expectedCounts = r._2



    }

    //TODO: HERE ADD THE ADJUSTMENT
    /*
      WE WANT TOP VALUES (LAST (N-TH VALUE) ,...(N+1)TH VALUE = SUFFICIENT GAP (THE GAP CAN BE CALCULATED
      //VIA STD.DEV. WE JUST TAKE THE VALUE WITH THE COUNT MATCHING THE LOWER BOUND OF THE STD. DEV
      //
     */
    PerformanceCounters.pruningThreshold(pruningThreshold)
    partialSumOnCounts(pruningThreshold, countsOfCounts)
    //Clear the counts
    val buffer = Array.ofDim[KNN](expectedCounts)
    /*
bucketIds: Array[Int],
                                            pointCounts: Array[Int],
                                            maxIndex: Int,
                                            countsOfCounts: Array[Int],
                                            remainingPoints: IntArrayBuffer,
                                            pruningThreshold: Int,
                                            buffer: Array[KNN]
     */
    clearCountsWithoutWeightsAfterPruning(bucketIds, pointCounts, maxIndex, countsOfCounts,  touchedPoints, pruningThreshold, buffer)

    //clearCountsWithoutWeights(bucketIds, pointCounts, pruningThreshold, maxIndex, countsOfCounts, buffer)

    PerformanceCounters.thresholdedPointsDuringSearch(buffer.size)

    //val sortedArray = java.util.Arrays.sort .sortBy(-_.similarity) //notice doing it by similarity
    PerformanceCounters.sortedPointsDuringSearch(buffer.size)

    fillDistances(query, settings.pointScoreSettings.rescoreExactlyTopK, buffer, settings, dataset)

    java.util.Arrays.sort(buffer, new KNNDistanceComparator())
    //val topK: Array[KVPair] = buffer.take(k)
    //val knns = topK.map({(kv: KVPair) => KNN(k = k, neighborId = kv.key, count = kv.value, label = id2Label(kv.key), dist = kv.value)})

    KNNS(k, pointName, buffer)
  }

}
