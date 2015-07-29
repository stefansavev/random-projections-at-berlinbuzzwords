package com.stefansavev.randomprojections.examples

import com.stefansavev.randomprojections.implementation.indexing.IndexBuilder
import com.stefansavev.randomprojections.implementation.{IndexSettings, ProjectionStrategies, NonThreadSafeSearcher, SearcherSettings}
import com.stefansavev.randomprojections.implementation.bucketsearch.{PointScoreSettings, PriorityQueueBasedBucketSearchSettings}
import com.stefansavev.randomprojections.tuning.PerformanceCounters
import com.stefansavev.randomprojections.evaluation.Evaluation
import com.stefansavev.randomprojections.utils.{Utils, AllNearestNeighborsForDataset}

object TestOnRandomData {

  def main (args: Array[String]): Unit = {
    val dataGenSettings = RandomBitStrings.RandomBitSettings(
      numGroups = 10000,
      numRowsPerGroup = 2,
      numCols=256,
      per1sInPrototype = 0.5,
      perNoise = 0.2)

      val debug = false
      val randomBitStringsDataset = RandomBitStrings.genRandomData(58585, dataGenSettings, debug, true)

      val randomTreeSettings = IndexSettings(
        maxPntsPerBucket=10,
        numTrees=10,
        maxDepth = None,
        projectionStrategyBuilder = ProjectionStrategies.dataInformedProjectionStrategy(), //.splitIntoKRandomProjection(2),
        randomSeed = 39393
      )

      println("Number of Rows: " + randomBitStringsDataset.numRows)

      val trees = Utils.timed("Build Index", {
        //IndexBuilder.buildWithPreprocessing(256, settings = randomTreeSettings,dataFrameView = randomBitStringsDataset)
        IndexBuilder.build(settings = randomTreeSettings,dataFrameView = randomBitStringsDataset)
      }).result

      val searcherSettings = SearcherSettings (
        bucketSearchSettings = PriorityQueueBasedBucketSearchSettings(numberOfRequiredPointsPerTree = 1000),
        pointScoreSettings = PointScoreSettings(topKCandidates = 100, rescoreExactlyTopK = 100),
        randomTrees = trees,
        trainingSet = randomBitStringsDataset)

      val searcher = new NonThreadSafeSearcher(searcherSettings)
      PerformanceCounters.initialize()

      val allNN = Utils.timed("Search All NN", {
        AllNearestNeighborsForDataset.getTopNearestNeighborsForAllPoints(10, searcher)
      }).result

      PerformanceCounters.report()

      Evaluation.evaluate(randomBitStringsDataset.getAllLabels(), allNN, -1, 1)
  }
}

