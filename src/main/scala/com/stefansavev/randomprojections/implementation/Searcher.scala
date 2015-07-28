package com.stefansavev.randomprojections.implementation

import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.implementation.bucketsearch._
import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer
import com.stefansavev.randomprojections.tuning.PerformanceCounters

case class SearcherSettings(bucketSearchSettings: BucketSearchSettings,
                            pointScoreSettings: PointScoreSettings,
                            randomTrees: RandomTrees,
                            trainingSet: DataFrameView,
                            usesPointWeights: Boolean = false){
  def createBucketSearchStrategy(): BucketSearchStrategy = {
    bucketSearchSettings match {
      case pqBasedSettings: PriorityQueueBasedBucketSearchSettings => new PriorityQueueBasedBucketSearchStrategy(randomTrees.datasetSplitStrategy, pqBasedSettings)
    }
  }
}

trait Searcher {
  def getNearestNeighborsByVector(query: Array[Double], topNearestNeighbors: Int): KNNS
  def getNearestNeighborsById(id: Int, topNearestNeighbors: Int): KNNS
  def getSettings(): SearcherSettings
}

class NonThreadSafeSearcher(settings: SearcherSettings) extends Searcher{
  if (!settings.randomTrees.header.isCompatible(settings.trainingSet.rowStoredView.getColumnHeader)){
    throw new IllegalStateException("Index and dataset are not compatible")
  }

  val numCols = settings.trainingSet.numCols //TODO: get those from the random tree
  val numRows = settings.trainingSet.numRows
  val scratchBuffer = new NearestNeigbhorQueryScratchBuffer(numRows, numCols, settings.usesPointWeights)
  val bucketSearchStrategy = settings.createBucketSearchStrategy()
  val randomTrees = settings.randomTrees
  val trainingSet = settings.trainingSet

  override def getNearestNeighborsByVector(query: Array[Double], topNearestNeighbors: Int): KNNS = {
    PerformanceCounters.processQuery()
    PerformanceCounters.numTrees(randomTrees.trees.length)
    val bucketSearchResult = bucketSearchStrategy.getBucketIndexes(randomTrees, query, scratchBuffer)
    PerformanceCounters.exploreBuckets(bucketSearchResult.bucketIndexBuffer.size)
    val pointIdHint = -1
    val knns = randomTrees.invertedIndex.getNearestNeighbors(topNearestNeighbors, pointIdHint, query, settings, bucketSearchResult, scratchBuffer)
    knns
  }

  def getNearestNeighborsById(id: Int, topNearestNeighbors: Int): KNNS = {
    val query = trainingSet.getPointAsDenseVector(id)
    val knn = getNearestNeighborsByVector(query, topNearestNeighbors)
    knn.copy(pointId = id) //must replace the id of the point
  }

  override def getSettings(): SearcherSettings = settings

}