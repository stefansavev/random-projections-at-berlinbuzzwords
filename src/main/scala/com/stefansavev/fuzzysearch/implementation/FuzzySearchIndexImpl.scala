package com.stefansavev.fuzzysearch.implementation

import java.io.File
import java.util

import com.stefansavev.fuzzysearch.FuzzySearchEngines.FuzzyIndexValueSize
import com.stefansavev.fuzzysearch.{FuzzySearchItem, FuzzySearchResult}
import com.stefansavev.randomprojections.datarepr.dense._
import com.stefansavev.randomprojections.dimensionalityreduction.svd.{SVDTransform, OnlineSVDFitter}
import com.stefansavev.randomprojections.implementation.bucketsearch.{PointScoreSettings, PriorityQueueBasedBucketSearchSettings}
import com.stefansavev.randomprojections.implementation.indexing.IndexBuilder
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.serialization.{DataFrameViewSerializationExt, DataFrameViewSerializers, RandomTreesSerialization}
import com.stefansavev.randomprojections.utils.Utils

class FuzzySearchIndexWrapper(trees: RandomTrees, dataset: DataFrameView) {
  val searcherSettings = SearcherSettings (
    bucketSearchSettings = PriorityQueueBasedBucketSearchSettings(numberOfRequiredPointsPerTree = 100),
    pointScoreSettings = PointScoreSettings(topKCandidates = 100, rescoreExactlyTopK = 100),
    randomTrees = trees,
    trainingSet = dataset)

  val searcher = new NonThreadSafeSearcher(searcherSettings)

  def bruteForceSearch(numNeighbors: Int, query: Array[Double]): java.util.List[FuzzySearchResult] = {
    import java.util.PriorityQueue
    val pq = new PriorityQueue[PointScore]()
    var j = 0
    val numRows = dataset.numRows
    while(j < numRows) { //TODO: probably you want to process in a randomized order to make sure you don't insert too many itmes in pq
      val score = dataset.cosineForNormalizedData(query, j)
      if (pq.size() < numNeighbors ){
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
    val n = pq.size()
    val sorted = Array.ofDim[PointScore](n)
    j = 0
    while(pq.size() > 0){
      sorted(n - j - 1) = pq.remove()
      j += 1
    }
    val sortedPoints = sorted //sorted.sortBy(ps => - ps.score)
    val result = new java.util.ArrayList[FuzzySearchResult]()
    var i = 0
    while(i < sortedPoints.length){
      val idAndScore = sortedPoints(i)
      val id = idAndScore.pointId
      val name = dataset.getName(id)
      val label = dataset.getLabel(id)
      result.add(new FuzzySearchResult(name, label, idAndScore.score))
      i += 1
    }
    result
  }

  def getNearestNeighborsByQuery(numNeighbors: Int, query: Array[Double]): java.util.List[FuzzySearchResult] = {
    val knns = searcher.getNearestNeighborsByVector(query, numNeighbors)
    val neighbors = knns.neighbors
    val dataset = this.dataset
    val result = new java.util.ArrayList[FuzzySearchResult]()
    var i = 0
    while(i < neighbors.length){
      val nn = neighbors(i)
      val id = nn.neighborId
      val name = dataset.getName(id)
      val label = nn.label
      result.add(new FuzzySearchResult(name, label, nn.dist))
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

  def getItemByName(name: String): FuzzySearchItem = {
    val i = dataset.getRowIdByName(name)
    getItemById(i)
  }

  def getItemById(i: Int): FuzzySearchItem = {
    new FuzzySearchItem(dataset.getName(i), dataset.getLabel(i), dataset.getPointAsDenseVector(i))
  }

  def getDimension(): Int = {
    dataset.numCols
  }

  def getItems(): java.util.Iterator[FuzzySearchItem] = {
    new util.Iterator[FuzzySearchItem] {
      val numRows = dataset.numRows
      var i = 0

      override def next(): FuzzySearchItem = {
        val item = getItemById(i)
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

class FuzzySearchIndexBuilderWrapper(dim: Int, numTrees: Int, valueSize: FuzzyIndexValueSize){
  val storageType = valueSize match  {
    case FuzzyIndexValueSize.AsDouble => {
      StoreBuilderAsDoubleType
    }
    case FuzzyIndexValueSize.As2Byte => {
      StoreBuilderAsBytesType
    }
    case FuzzyIndexValueSize.AsSingleByte => {
      StoreBuilderAsSingleByteType
    }
  }

  val columnIds = Array.range(0, dim)
  val header = ColumnHeaderBuilder.build("label", columnIds.map(i => ("f" + i, i)), true, storageType)
  val builder = DenseRowStoredMatrixViewBuilderFactory.create(header)
  val onlineSVDFitter = new OnlineSVDFitter(dim)

  def addItem(name: String, label: Int, dataPoint: Array[Double]): Unit = {
    onlineSVDFitter.pass(dataPoint)
    builder.addRow(name, label, columnIds, dataPoint)
  }

  def build(): FuzzySearchIndexWrapper = {
    val numRows = builder.currentRowId
    val indexes = PointIndexes(Array.range(0, numRows))
    val dataset = new DataFrameView(indexes, builder.asInstanceOf[DenseRowStoredMatrixViewBuilder].build())

    val svdTransform = onlineSVDFitter.finalizeFit().asInstanceOf[SVDTransform]

    val randomTreeSettings = IndexSettings(
      maxPntsPerBucket=50,
      numTrees=numTrees,
      maxDepth = None,
      projectionStrategyBuilder = ProjectionStrategies.splitIntoKRandomProjection(8),
      reportingDistanceEvaluator = ReportingDistanceEvaluators.cosineOnOriginalData(),
      randomSeed = 39393,
      signatureSize = 16
    )
    println(dataset)

    val trees = Utils.timed("Create trees", {
      IndexBuilder.buildWithSVDAndRandomRotation(32, settings = randomTreeSettings, dataFrameView = dataset, Some(svdTransform))
    }).result
    new FuzzySearchIndexWrapper(trees, dataset)
  }
}

