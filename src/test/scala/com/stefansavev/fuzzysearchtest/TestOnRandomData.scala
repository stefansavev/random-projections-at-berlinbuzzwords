package com.stefansavev.fuzzysearchtest

import java.util.Random

import com.stefansavev.randomprojections.actors.Application
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.utils.Utils
import com.stefansavev.similaritysearch.SimilaritySearchEvaluationUtils
import com.stefansavev.similaritysearch.VectorType.StorageSize
import com.stefansavev.similaritysearch.implementation.FuzzySearchIndexBuilderWrapper
import com.typesafe.scalalogging.StrictLogging


object TestOnRandomData extends StrictLogging {
  implicit val _ = logger

  def main(args: Array[String]): Unit = {
    val dataGenSettings = RandomBitStrings.RandomBitSettings(
      numGroups = 100000,
      numRowsPerGroup = 2,
      numCols = 256,
      per1sInPrototype = 0.5,
      perNoise = 0.1)

    val debug = false
    val randomBitStringsDataset = RandomBitStrings.genRandomData(58585, dataGenSettings, debug, true)

    val randomTreeSettings = IndexSettings(
      maxPntsPerBucket = 50,
      numTrees = 50,
      maxDepth = None,
      projectionStrategyBuilder = ProjectionStrategies.splitIntoKRandomProjection(),
      reportingDistanceEvaluator = ReportingDistanceEvaluators.cosineOnOriginalData(),
      randomSeed = 39393
    )

    println("Number of Rows: " + randomBitStringsDataset.numRows)
    val diskLocation = "D:/tmp/randomfile"
    val trees = Utils.timed("Build Index", {
      val wrapper = new FuzzySearchIndexBuilderWrapper(diskLocation, randomBitStringsDataset.numCols, 50, StorageSize.Double)
      var i = 0
      while (i < randomBitStringsDataset.numRows) {
        wrapper.addItem(i.toString, 0, randomBitStringsDataset.getPointAsDenseVector(i))
        i += 1
      }
      wrapper.build()
      //SimilaritySearchIndex.open(diskLocation)
      ()
    }).result

    SimilaritySearchEvaluationUtils.compareWithBruteForce(diskLocation, new Random(481868), 1000, 50)

    /*
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
      */

    Application.shutdown()
  }
}

