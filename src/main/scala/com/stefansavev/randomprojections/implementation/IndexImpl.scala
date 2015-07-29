package com.stefansavev.randomprojections.implementation

import java.io.File
import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer
import com.stefansavev.randomprojections.interface.Index
import com.stefansavev.randomprojections.serialization.{PointSignaturesSerializer, BinaryFileSerializer}
import com.stefansavev.randomprojections.tuning.PerformanceCounters

class IndexImpl(val signatures: PointSignatures, val totalNumPoints: Int, val leaf2Points: Leaf2Points, val id2Label: Array[Int]) extends Index{
  def toFile(file: File): Unit = {
    val ser = new BinaryFileSerializer(file)
    PointSignaturesSerializer.toBinary(ser.stream, signatures)
    ser.putIntArrays(leaf2Points.starts, leaf2Points.points)
    ser.putInt(leaf2Points.numberOfLeaves())

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


  override def getNearestNeighbors(k: Int, pointName: Int, query: Array[Double], settings: SearcherSettings, searchBucketsResult: SearchBucketsResult, scratchBuffer: NearestNeigbhorQueryScratchBuffer): KNNS = {
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
  /*
  val sortedArray = if (pruningThreshold == 0) buffer.toArray.sortBy({case (k,v) => (-v, k)}) else buffer.toArray.sortBy(-_._2)
  val knns = sortedArray.take(k).map({case (pointId, count) => KNN(k = k, neighborId = pointId, count = count, label = id2Label(pointId))})
  */
  /*
  val arrPntIds = buffPntIds.toArray()
  val cntPntIds = buffCnts.toArray()
  var numNNReported = if (arrPntIds.length > 0) 1 else 0
  val knns = Array.ofDim[KNN](numNNReported)
  var z = 0
  var maxId = -1
  var maxCnt = -1
  while(z < arrPntIds.length){
    val cnt = cntPntIds(z)
    if (cnt > maxCnt){
      maxCnt = cnt
      maxId = arrPntIds(z)
    }
    z += 1
  }
  if (numNNReported > 0){
    knns(0) = KNN(k, maxId, maxCnt, id2Label(maxId))
  }

  KNNS(k, pointName, knns)
}
*/
}
