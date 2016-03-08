package com.stefansavev.examples.randomdata

import com.stefansavev.examples.mnist.MnistDigitsAfterSVD._
import com.stefansavev.randomprojections.implementation.indexing.IndexBuilder
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.implementation.bucketsearch.{PointScoreSettings, PriorityQueueBasedBucketSearchSettings}
import com.stefansavev.randomprojections.tuning.PerformanceCounters
import com.stefansavev.randomprojections.evaluation.Evaluation
import com.stefansavev.randomprojections.utils.{Utils, AllNearestNeighborsForDataset}
import com.typesafe.scalalogging.StrictLogging


object TestOnRandomData extends StrictLogging{
  implicit val _ = logger

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
        maxPntsPerBucket=50,
        numTrees=50,
        maxDepth = None,
        projectionStrategyBuilder = ProjectionStrategies.splitIntoKRandomProjection(),
        reportingDistanceEvaluator = ReportingDistanceEvaluators.cosineOnOriginalData(),
        randomSeed = 39393
      )

      println("Number of Rows: " + randomBitStringsDataset.numRows)

      val trees = Utils.timed("Build Index", {
        IndexBuilder.buildWithSVDAndRandomRotation(null, 32, settings = randomTreeSettings,dataFrameView = randomBitStringsDataset)
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

