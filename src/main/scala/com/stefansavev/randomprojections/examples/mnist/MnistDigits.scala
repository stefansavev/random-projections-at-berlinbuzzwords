package com.stefansavev.randomprojections.examples.mnist

import com.stefansavev.randomprojections.datarepr.dense._
import com.stefansavev.randomprojections.file.CSVFileOptions
import com.stefansavev.randomprojections.implementation.indexing.IndexBuilder
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.implementation.bucketsearch.{PointScoreSettings, PriorityQueueBasedBucketSearchSettings}
import com.stefansavev.randomprojections.serialization.RandomTreesSerialization
import com.stefansavev.randomprojections.tuning.PerformanceCounters
import com.stefansavev.randomprojections.evaluation.Evaluation
import com.stefansavev.randomprojections.utils.{Utils, AllNearestNeighborsForDataset}
import com.stefansavev.randomprojections.examples.ExamplesSettings


object MnistDigits {
  import RandomTreesSerialization.Implicits._

  def main (args: Array[String]): Unit = {
    val inputFile = Utils.combinePaths(ExamplesSettings.inputDirectory, "mnist/originaldata/train.csv")
    val indexFile = Utils.combinePaths(ExamplesSettings.outputDirectory, "mnist/originaldata-index")

    val dataset = MnistUtils.loadData(inputFile)
    val doTrain = true
    val doSearch = true

    val randomTreeSettings = IndexSettings(
      maxPntsPerBucket=100,
      numTrees=40,
      maxDepth = None,
      projectionStrategyBuilder = ProjectionStrategies.splitIntoKRandomProjection(16), //.dataInformedProjectionStrategy(), // .splitIntoKRandomProjection(16),
      reportingDistanceEvaluator = ReportingDistanceEvaluators.cosineOnOriginalData(),
      randomSeed = 39393
    )

    println(dataset)

    if (doTrain) {
      val trees =
          Utils.timed("Build index", {
          IndexBuilder.buildWithPreprocessing(/*4 * */128, settings = randomTreeSettings, dataFrameView = dataset)
        }).result
      trees.toFile(indexFile)
    }

    if (doSearch){
      val treesFromFile = RandomTrees.fromFile(indexFile)

      val searcherSettings = SearcherSettings (
        bucketSearchSettings = PriorityQueueBasedBucketSearchSettings(numberOfRequiredPointsPerTree = 100),
        pointScoreSettings = PointScoreSettings(topKCandidates = 100, rescoreExactlyTopK = 100),
        randomTrees = treesFromFile,
        trainingSet = dataset)

      val searcher = new NonThreadSafeSearcher(searcherSettings)
      PerformanceCounters.initialize()
      val allNN = Utils.timed("All nearest neighbors", {
        AllNearestNeighborsForDataset.getTopNearestNeighborsForAllPoints(100, searcher)
      }).result
      PerformanceCounters.report()
      Evaluation.evaluate(dataset.getAllLabels(), allNN, -1, 1)
    }
  }
}

