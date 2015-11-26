package com.stefansavev.randomprojections.examples.wordvec

import com.stefansavev.fuzzysearch.implementation.FuzzySearchResultsWrapper
import com.stefansavev.fuzzysearch.{FuzzySearchResults, FuzzySearchResult, FuzzySearchResultBuilder}
import com.stefansavev.randomprojections.datarepr.dense._
import com.stefansavev.randomprojections.file.{CSVFile, CSVFileOptions}
import com.stefansavev.randomprojections.implementation.indexing.IndexBuilder
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.implementation.bucketsearch.{PointScoreSettings, PriorityQueueBasedBucketSearchSettings}
import com.stefansavev.randomprojections.tuning.PerformanceCounters
import com.stefansavev.randomprojections.evaluation.{RecallEvaluator, Evaluation}
import com.stefansavev.randomprojections.utils.{Utils, AllNearestNeighborsForDataset}

import scala.collection.mutable.ArrayBuffer
import com.stefansavev.randomprojections.examples.{WordVecGroundTruth, ExamplesSettings}

object WordVecs {
  def fromFile(fileName: String,  opt: CSVFileOptions, dataFrameOptions: DataFrameOptions,  matrixBuilderFactory: RowStoredMatrixViewBuilderFactory, topK: Int): DataFrameView = {
    if (opt.hasHeader){
      throw new IllegalStateException("Header cannot be set for this data")
    }

    val file = CSVFile.read(fileName, opt)
    if (file.header.isDefined){
      throw new IllegalStateException("Header must be present")
    }

    def getTransformer(columnName: String): RowTransformer = {
        new DoubleTransformer(columnName)
    }

    val(featureIndexes: Array[(Int,RowTransformer)], columnWithDataFrameIndexes: Array[(String,Int)]) =
        Array.range(0,200).map((i: Int) => ("f" + i, i)).
        map({case ((columnName: String, i)) =>
        ((i, getTransformer(columnName)), (columnName, i))
      }).unzip

    val header = ColumnHeaderBuilder.build("label", columnWithDataFrameIndexes, true)
    val builder = matrixBuilderFactory.create(header)

    var numRows = 0

    val labelTransformer = new IntTransformer("label")
    //val buff = new ArrayBuffer[String]()
    for(valuesByLine1 <- file.getLines().take(topK)){
      val line = numRows + 2
      //buff += valuesByLine1(0)
      val word = valuesByLine1(0)
      val valuesByLine = valuesByLine1(1).split(" ")
      val label = 0
      var j = 0
      val indexes = Array.ofDim[Int](featureIndexes.length)
      val values = Array.ofDim[Double](featureIndexes.length)
      var norm = 0.0
      while(j < featureIndexes.length){
        val rowTransformer = featureIndexes(j)
        val index = rowTransformer._1
        val transformer = rowTransformer._2
        val value = valuesByLine(index)
        val dblValue = transformer.transformToDouble(line, index, value)
        indexes(j) = j
        values(j) = dblValue
        norm += dblValue*dblValue
        j += 1
      }

      builder.addRow(word, label, indexes, values)
      numRows += 1
      if (numRows % 1000 == 0){
        println("Line " + numRows)
      }
    }
    file.close()
    //val newHeader = ColumnHeaderBuilder.build("label", columnWithDataFrameIndexes, false)
    val b = builder.asInstanceOf[DenseRowStoredMatrixViewBuilder].build()

    val indexes = PointIndexes(Array.range(0, numRows))
    new DataFrameView(indexes, b)
  }

  //word vectors
  //The data for this task is a result of running the sample command https://code.google.com/p/word2vec/
  //There are 71291. For each of them this code computes approximation of NN
  def main (args: Array[String]): Unit = {

    val opt = CSVFileOptions(sep = "\t", onlyTopRecords = None, hasHeader = false)
    val inputFile = Utils.combinePaths(ExamplesSettings.inputDirectory, "wordvec/wordvec.txt")
    val outputFileNN = Utils.combinePaths(ExamplesSettings.outputDirectory, "wordvec-nearest-neighbors.txt")

    val numColumns = 18828 //hard-coded at the moment
    val dataFrameOptions = new DataFrameOptions("label", false, null)
    val dataset = WordVecs.fromFile(inputFile, opt, dataFrameOptions, DenseRowStoredMatrixViewBuilderFactory, 100000)

    val c = new FuzzySearchResults(FuzzySearchResultsWrapper.fromTextFile("C:/tmp/word-vec-truth.txt"))

    val truth1 = WordVecGroundTruth.readGroundTruthInObject().mapping
    val builderNeighbors = new FuzzySearchResultBuilder()
    val rowNames = dataset.getAllRowNames()
    for(name <- rowNames){
      val neighbors = truth1(name).sortBy(-_._2)
      val list = new java.util.ArrayList[FuzzySearchResult]()
      for(kv <- neighbors){
        list.add(new FuzzySearchResult(kv._1, -1, kv._2))
      }
      builderNeighbors.addResult(name, list)
    }
    val b = builderNeighbors.build()
    //builderNeighbors.build().toTextFile("C:/tmp/word-vec-truth.txt")

    val r = RecallEvaluator.evaluateRecall(10, b, c)
    r.printRecalls()
    throw new IllegalStateException("done!!!")

    val debug =false
    val randomTreeSettings = IndexSettings(
      maxPntsPerBucket=50,
      numTrees=50,
      maxDepth = None,
      projectionStrategyBuilder = ProjectionStrategies.splitIntoKRandomProjection(8),
      reportingDistanceEvaluator = ReportingDistanceEvaluators.cosineOnOriginalData(),
      randomSeed = 39393
    )
    println(dataset)

    val trees = Utils.timed("Build index", {
      IndexBuilder.buildWithSVDAndRandomRotation(null, 32, settings = randomTreeSettings,dataFrameView = dataset)
    }).result

    val searcherSettings = SearcherSettings (
      bucketSearchSettings = PriorityQueueBasedBucketSearchSettings(numberOfRequiredPointsPerTree = 100),
      pointScoreSettings = PointScoreSettings(topKCandidates = 100, rescoreExactlyTopK = 100),
      randomTrees = trees,
      trainingSet = dataset)

    val searcher = new NonThreadSafeSearcher(searcherSettings)
    PerformanceCounters.initialize()

    val allNN = Utils.timed("Search for NN", {
      AllNearestNeighborsForDataset.getTopNearestNeighborsForAllPoints(100, searcher)
    }).result

    PerformanceCounters.report()

    Evaluation.dumpLabels(outputFileNN, dataset.rowStoredView.getAllRowNames(), allNN, 0, 10)

    val truth2 = WordVecGroundTruth.readGroundTruthInObject()
    WordVecGroundTruth.evaluateRecall(allNN, truth2, dataset, 10)

  }
}
