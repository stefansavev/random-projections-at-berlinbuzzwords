package com.stefansavev.fuzzysearch.implementation

import java.io.File
import java.util

import com.stefansavev.fuzzysearch.{FuzzySearchItem, NearestNeighbor}
import com.stefansavev.randomprojections.datarepr.dense._
import com.stefansavev.randomprojections.implementation.bucketsearch.{PointScoreSettings, PriorityQueueBasedBucketSearchSettings}
import com.stefansavev.randomprojections.implementation.indexing.IndexBuilder
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.serialization.{DataFrameViewSerializationExt, DataFrameViewSerializers, RandomTreesSerialization}
import com.stefansavev.randomprojections.utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class FuzzySearchIndexWrapper(trees: RandomTrees, dataset: DataFrameView) {
  val searcherSettings = SearcherSettings (
    bucketSearchSettings = PriorityQueueBasedBucketSearchSettings(numberOfRequiredPointsPerTree = 100),
    pointScoreSettings = PointScoreSettings(topKCandidates = 100, rescoreExactlyTopK = 100),
    randomTrees = trees,
    trainingSet = dataset)

  val searcher = new NonThreadSafeSearcher(searcherSettings)

  def getNearestNeighborsByQuery(numNeighbors: Int, query: Array[Double]): java.util.List[NearestNeighbor] = {
    val knns = searcher.getNearestNeighborsByVector(query, numNeighbors)
    val neighbors = knns.neighbors
    val dataset = this.dataset
    val result = new java.util.ArrayList[NearestNeighbor]()
    var i = 0
    while(i < neighbors.length){
      val nn = neighbors(i)
      val id = nn.neighborId
      val name = dataset.getName(id)
      val label = nn.label
      result.add(new NearestNeighbor(name, label, nn.dist))
      i += 1
    }
    result
  }

  def save(fileName: String): Unit = {
    import RandomTreesSerialization.Implicits._
    import DataFrameViewSerializationExt._
    val file = new File(fileName)
    if (!file.exists()) {
      throw new IllegalStateException(s"Directory should exist: ${fileName}")
    }

    if (!file.isDirectory()){
      throw new IllegalStateException(s"File is not a directory ${fileName}")
    }
    val treesFullDir = new File(fileName, FuzzySearchIndexWrapper.treesSubDir)
    if (!treesFullDir.exists()){
      treesFullDir.mkdir()
    }

    val datasetFullFile = new File(fileName, FuzzySearchIndexWrapper.datasetFile)
    trees.toFile(treesFullDir)
    dataset.toFile(datasetFullFile)
  }

  def getItemByName(name: String):FuzzySearchItem = {
    null
  }

  def getDimension(): Int = {
    dataset.numCols
  }

  def getItems(): java.util.Iterator[FuzzySearchItem] = {
    new util.Iterator[FuzzySearchItem] {
      val numRows = dataset.numRows
      var i = 0

      override def next(): FuzzySearchItem = {
        val item = new FuzzySearchItem(dataset.getName(i), dataset.getLabel(i), dataset.getPointAsDenseVector(i))
        i += 1
        item
      }

      override def remove(): Unit = {
        throw new IllegalStateException("remove is not supported")
      }

      override def hasNext: Boolean = {
        i < numRows
      }
    }

  }

}

object FuzzySearchIndexWrapper{
  val treesSubDir = "fuzzysearch_trees_dir_8686116"
  val datasetFile = "fuzzysearch_dataset_file_8686116"

  def open(fileName: String): FuzzySearchIndexWrapper = {
    import RandomTreesSerialization.Implicits._
    import DataFrameViewSerializationExt._

    val treesFullDir = new File(fileName, treesSubDir)
    val datasetFullFile = new File(fileName, datasetFile)

    val trees = RandomTrees.fromFile(treesFullDir)
    val dataset = DataFrameView.fromFile(datasetFullFile)
    new FuzzySearchIndexWrapper(trees, dataset)
  }
}

class FuzzySearchIndexBuilderWrapper(dim: Int, numTrees: Int){
  val columnIds = Array.range(0, dim)
  val header = ColumnHeaderBuilder.build("label", columnIds.map(i => ("f" + i, i)), Array.empty)
  val names = new ArrayBuffer[String]()
  val builder = DenseRowStoredMatrixViewBuilderFactory.create(header)

  def addItem(name: String, label: Int, dataPoint: Array[Double]): Unit = {
    names += name
    builder.addRow(label, columnIds, dataPoint)
  }

  def build(): FuzzySearchIndexWrapper = {
    builder.build()
    val numRows = names.size
    val indexes = PointIndexes(Array.range(0, numRows))
    val modifiedHeader = ColumnHeaderBuilder.build("label", columnIds.map(i => ("f" + i, i)), names.toArray)
    val dataset = new DataFrameView(indexes, builder.asInstanceOf[DenseRowStoredMatrixViewBuilder].build(modifiedHeader))


    val randomTreeSettings = IndexSettings(
      maxPntsPerBucket=50,
      numTrees=numTrees,
      maxDepth = None,
      projectionStrategyBuilder = ProjectionStrategies.splitIntoKRandomProjection(4),
      reportingDistanceEvaluator = ReportingDistanceEvaluators.cosineOnOriginalData(),
      randomSeed = 39393
    )
    println(dataset)


    val trees = Utils.timed("Create trees", {
      IndexBuilder.buildWithSVDAndRandomRotation(32, settings = randomTreeSettings, dataFrameView = dataset)
    }).result
    new FuzzySearchIndexWrapper(trees, dataset)
  }
}

