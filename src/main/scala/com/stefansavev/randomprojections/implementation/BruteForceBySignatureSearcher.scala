package com.stefansavev.randomprojections.implementation

import com.stefansavev.randomprojections.tuning.PerformanceCounters

object Counter{
  var i = 0
}

class PointScore(val pointId: Int, val score: Double) extends Comparable[PointScore] {
  override def compareTo(o: PointScore): Int = {
    //o.score.compareTo(this.score)
    this.score.compareTo(o.score)
  }
}
class BruteForceBySignatureSearcher(settings: SearcherSettings) extends Searcher{
  if (!settings.randomTrees.header.isCompatible(settings.trainingSet.rowStoredView.getColumnHeader)){
    throw new IllegalStateException("Index and dataset are not compatible")
  }
  /*
  val numCols = settings.trainingSet.numCols //TODO: get those from the random tree
  val numRows = settings.trainingSet.numRows
  val scratchBuffer = new NearestNeigbhorQueryScratchBuffer(numRows, numCols, settings.usesPointWeights)
  val bucketSearchStrategy = settings.createBucketSearchStrategy()
  val randomTrees = settings.randomTrees
  */
  val trainingSet = settings.trainingSet
  val numRows = settings.trainingSet.numRows
  val signatureVectors = settings.randomTrees.signatureVecs
  val signatures = settings.randomTrees.invertedIndex.asInstanceOf[IndexImpl].signatures

  def selectTopKNeighbors(query: Array[Double], k: Int): Array[PointScore] = {
    val querySig = signatureVectors.computePointSignatures(query)
    import java.util.PriorityQueue
    val pq = new PriorityQueue[PointScore]()
    var j = 0
    while(j < numRows) { //TODO: probably you want to process in a randomized order to make sure you don't insert too many itmes in pq
      val score = signatures.overlap(querySig, j)
      if (pq.size() < k ){
        pq.add(new PointScore(j, score))
      }
      else{//it should be exactly k items in pq
        val minAcceptedScore = pq.peek().score
        if (score > minAcceptedScore){
          pq.remove() //remove min
          pq.add(new PointScore(j, score))
        }
      }
      j += 1
    }
    val sorted = Array.ofDim[PointScore](pq.size())
    j = 0
    while(pq.size() > 0){
      sorted(j) = pq.remove()
      j += 1
    }
    sorted.sortBy(ps => - ps.score)
  }


  override def getNearestNeighborsByVector(query: Array[Double], topNearestNeighbors: Int): KNNS = {
    val start = System.currentTimeMillis()
    val topNeighbors = selectTopKNeighbors(query, topNearestNeighbors)
    val asKNN = topNeighbors.map(ps => KNN(ps.pointId, ps.score.toInt, trainingSet.getLabel(ps.pointId), ps.score))
    val end = System.currentTimeMillis()
    Counter.i += 1
    PerformanceCounters.processQuery(end - start)
    KNNS(10, -1, asKNN)
  }

  def getNearestNeighborsById(id: Int, topNearestNeighbors: Int): KNNS = {
    val query = trainingSet.getPointAsDenseVector(id)
    val knn = getNearestNeighborsByVector(query, topNearestNeighbors)
    knn.copy(pointId = id) //must replace the id of the point
  }

  override def getSettings(): SearcherSettings = settings
}