package com.stefansavev.randomprojections.examples

import java.io.PrintWriter
import com.stefansavev.randomprojections.datarepr.dense.{DataFrameOptions, DataFrameView, DenseRowStoredMatrixViewBuilderFactory, RowStoredMatrixView}
import com.stefansavev.randomprojections.file.CSVFileOptions
import com.stefansavev.randomprojections.implementation.indexing.IndexBuilder
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.implementation.bucketsearch.{PointScoreSettings, PriorityQueueBasedBucketSearchSettings}
import com.stefansavev.randomprojections.serialization.RandomTreesSerialization
import com.stefansavev.randomprojections.tuning.PerformanceCounters
import com.stefansavev.randomprojections.evaluation.Evaluation
import com.stefansavev.randomprojections.utils.{AllNearestNeighborsForDataset, Utils}

object MnistDigitsAfterSVD {
  import RandomTreesSerialization.Implicits._

  def loadData(fileName: String): DataFrameView ={
    val opt = CSVFileOptions(onlyTopRecords = None)
    val dataFrameOptions = new DataFrameOptions(labelColumnName = "label", builderFactory = DenseRowStoredMatrixViewBuilderFactory, normalizeVectors = true)
    RowStoredMatrixView.fromFile(fileName, opt, dataFrameOptions)
  }

  def main (args: Array[String]): Unit = {
    val trainFile = Utils.combinePaths(ExamplesSettings.inputDirectory, "mnist/svdpreprocessed/train.csv")
    val indexFile = Utils.combinePaths(ExamplesSettings.outputDirectory, "mnist/svdpreprocessed-index")
    val testFile = Utils.combinePaths(ExamplesSettings.inputDirectory, "mnist/svdpreprocessed/test.csv")
    val predictionsOnTestFile = Utils.combinePaths(ExamplesSettings.outputDirectory, "mnist/predictions-test-svd.csv")

    val doTrain = true
    val doSearch = true
    val doTest = true

    val dataset = {
      import com.stefansavev.randomprojections.serialization.DataFrameViewSerializationExt._
      val tmpFile = "D:/tmp/tmp-dataset-mnist"
      val dataset = loadData(trainFile)
      Utils.timed("Write to file", {dataset.toFile(tmpFile)})
      val dataset1 = Utils.timed("Read from file", {DataFrameView.fromFile(tmpFile)}).result
      dataset1
    }

    val randomTreeSettings = IndexSettings(
      maxPntsPerBucket=10,
      numTrees=10,
      maxDepth = None,
      projectionStrategyBuilder = ProjectionStrategies.splitIntoKRandomProjection(2),
      reportingDistanceEvaluator = ReportingDistanceEvaluators.cosineOnOriginalData(),
      randomSeed = 39393
    )
    println(dataset)

    if (doTrain) {
      val trees = Utils.timed("Create trees", {
        IndexBuilder.buildWithPreprocessing(64, settings = randomTreeSettings, dataFrameView = dataset)
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
      val allNN = Utils.timed("Search all Nearest neighbors", {
        AllNearestNeighborsForDataset.getTopNearestNeighborsForAllPoints(100, searcher)
      }).result

      PerformanceCounters.report()

      val accuracy = Evaluation.evaluate(dataset.getAllLabels(), allNN, -1, 1)
      if (Math.abs(accuracy - 97.55) > 0.001){
        throw new IllegalStateException("broke it")
      }
    }

    if (doTest){
      val testDataset = loadData(testFile)
      println(testDataset)
      val treesFromFile = RandomTrees.fromFile(indexFile)

      val searcherSettings = SearcherSettings (
        bucketSearchSettings = PriorityQueueBasedBucketSearchSettings(numberOfRequiredPointsPerTree = 100),
        pointScoreSettings = PointScoreSettings(topKCandidates = 100, rescoreExactlyTopK = 100),
        randomTrees = treesFromFile,
        trainingSet = dataset)

      val searcher = new NonThreadSafeSearcher(searcherSettings)

      PerformanceCounters.initialize()
      val allNN = Utils.timed("Search all Nearest neighbors", {
        AllNearestNeighborsForDataset.getTopNearestNeighborsForAllPointsTestDataset(100, searcher, testDataset)
      }).result
      writePredictionsInKaggleFormat(predictionsOnTestFile, allNN)
      //expected score on kaggle: 0.97557
    }
  }

  def writePredictionsInKaggleFormat(outputFile: String, allKnns: Array[KNNS]): Unit = {
    val writer = new PrintWriter(outputFile)
    writer.println("ImageId,Label")
    var i = 0
    while(i < allKnns.length){
      val topNNLabel = allKnns(i).neighbors(0).label
      val pointId = i + 1
      val asStr = s"${pointId},${topNNLabel}"
      writer.println(asStr)
      i += 1
    }
    writer.close()
  }
}
