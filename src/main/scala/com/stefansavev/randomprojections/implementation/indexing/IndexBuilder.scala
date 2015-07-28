package com.stefansavev.randomprojections.implementation.indexing

import java.util.Random
import com.stefansavev.randomprojections.buffers.IntArrayBuffer
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.interface.{BucketCollector, Index}
import com.stefansavev.randomprojections.interface.BucketCollector
import com.stefansavev.randomprojections.datarepr.dense._
import com.stefansavev.randomprojections.utils.Utils

trait Point2LeavesMap{
  def getTreeLeaves(pointId: Int): Array[(Int, Int)]
}

class Point2LeavesMapImpl(map: scala.collection.mutable.HashMap[Int, Array[(Int, Int)]]) extends Point2LeavesMap{
  def getTreeLeaves(pointId: Int): Array[(Int, Int)] = {
    map(pointId)
  }
}

object IndexBuilder{
  class Counter(){
    var index = 0

    def next(): Int = {
      index += 1
      index
    }
  }

  def buildTree(treeId: Int,
                 rnd: Random,
                 settings: IndexSettings,
                 inputDataView: DataFrameView,
                 collector: BucketCollector,
                 splitStrategy: DatasetSplitStrategy,
                 projStrategy: ProjectionStrategy,
                 logger: IndexCounters): RandomTree = {

    val counter = new Counter()
    def loop(dataFrameView: DataFrameView, depth: Int, noProgressCount: Int): RandomTree = {
      val hasReachedDepth = settings.maxDepth match {case None => false; case Some(v) => depth >= v}
      if (dataFrameView.numRows <= 0){
        EmptyLeaf //too little data
      }
      else if (hasReachedDepth || (dataFrameView.numRows < settings.maxPntsPerBucket || noProgressCount >= 10)) {
        if (noProgressCount >= 10){
          val numPointsInBucket = dataFrameView.getUnderlyingIndexes().size
          println(s"No progress in >= 10 iterations. Saving ${numPointsInBucket} datapoints to the bucket")
        }
        collector.collectPoints(dataFrameView.getUnderlyingIndexes())
      }
      else {
        val nodeId = counter.next() //for debugging purposes
        val projectionVector = projStrategy.nextRandomProjection(depth, dataFrameView)
        val (splits, means) = splitStrategy.splitDataset(dataFrameView, projectionVector)

        var m = 0
        var numFull = 0
        while(m < splits.length){
          if (splits(m).numRows > 0){
            numFull += 1
          }
          m += 1
        }
        if (numFull <= 1) {
          loop(dataFrameView, depth, noProgressCount + 1)
        }
        else {
          val children = Array.ofDim[RandomTree](splits.size)
          var count = 0
          var i = 0
          while (i < splits.length) {
            val slice = splits(i)
            if (slice.numRows > 0){
              val subTree = loop(slice, depth + 1, 0)
              children(i) = subTree
              count += subTree.getCount
            }

            i += 1
          }

          RandomTreeNode(nodeId, projectionVector, count, means, children)
        }
      }
    }
    loop(inputDataView, 0, 0)
  }



  def sameDataFrame(dataFrame: DataFrameView, projSt: ProjectionStrategy): (AbstractProjectionVector, DataFrameView) = {
    val onesV = new SparseVector(dataFrame.numCols, Array.range(0, dataFrame.numCols), Array.range(0, dataFrame.numCols).map(_ => 1.0))
    val proj = new HadamardProjectionVector(onesV)
    (proj, dataFrame)
  }

  def modifyDataFrame(dataFrame: DataFrameView, projSt: ProjectionStrategy): (AbstractProjectionVector, DataFrameView) = {
    val oldHeader = dataFrame.rowStoredView.getColumnHeader
    val newNumCols = projSt.asInstanceOf[SplitIntoKProjectionStrategy].k
    val newF = Array.range(0, newNumCols).map(i => (i.toString,i ))
    val header = ColumnHeaderBuilder.build(oldHeader.labelName, newF, Array.empty)
    val builder = DenseRowStoredMatrixViewBuilderFactory.create(header)
    val absprojVec = projSt.nextRandomProjection(0, dataFrame)
    val signs = absprojVec.asInstanceOf[HadamardProjectionVector].signs
    val vec = Array.ofDim[Double](dataFrame.numCols)
    val input = Array.ofDim[Double](signs.ids.length)
    val output = Array.ofDim[Double](signs.ids.length)
    val newIds = Array.range(0, newNumCols)
    var i = 0
    while(i < dataFrame.numRows){
      dataFrame.getPointAsDenseVector(i, signs.ids, vec)
      var j = 0
      while(j < signs.ids.length){
        val index = signs.ids(j)
        val b = signs.values(j)
        val a = vec(index)
        input(j) = a*b
        j += 1
      }
      HadamardUtils.multiplyInto(newNumCols, input, output)
      builder.addRow(i, newIds, output)
      i += 1
    }

    val indexes = dataFrame.indexes
    (absprojVec, new DataFrameView(indexes, builder.build()))
  }

  def build(settings: IndexSettings, dataFrameView: DataFrameView): RandomTrees = {
    val bucketCollector = new BucketCollectorImpl(dataFrameView.numRows)
    val rnd = new Random(settings.randomSeed)
    val projStrategy: ProjectionStrategy = settings.projectionStrategyBuilder.build(settings, rnd, dataFrameView)
    val splitStrategy = settings.projectionStrategyBuilder.datasetSplitStrategy
    val logger = new IndexCounters()
    val randomTrees = Array.ofDim[RandomTree](settings.numTrees)
    for(i <- 0 until settings.numTrees){
      val randomTree = Utils.timed(s"Build tree ${i}", {buildTree(i, rnd, settings, dataFrameView, bucketCollector, splitStrategy, projStrategy, logger)}).result
      randomTrees(i) = randomTree
    }
    new RandomTrees(splitStrategy, dataFrameView.rowStoredView.getColumnHeader, bucketCollector.build(dataFrameView.getAllLabels()), randomTrees)
  }

  def buildWithPreprocessing(numInterProj: Int, settings: IndexSettings, dataFrameView: DataFrameView): RandomTrees = {
    val bucketCollector = new BucketCollectorImpl(dataFrameView.numRows)
    val rnd = new Random(settings.randomSeed)
    val logger = new IndexCounters()
    val randomTrees = Array.ofDim[RandomTree](settings.numTrees)
    val emptyScores = Array.ofDim[Double](dataFrameView.indexes.indexes.length)
    val newIndexes = PointIndexes(dataFrameView.indexes.indexes)
    val newDataFrameView = new DataFrameView(newIndexes, dataFrameView.rowStoredView)
    val newProjS = ProjectionStrategies.splitIntoKRandomProjection(numInterProj).build(settings, rnd, dataFrameView)
    var savedRes: (AbstractProjectionVector, DataFrameView) = null
    val splitStrategy = settings.projectionStrategyBuilder.datasetSplitStrategy
    for(i <- 0 until settings.numTrees){
      savedRes = modifyDataFrame(newDataFrameView, newProjS)
      val (absProjV, modDF) = savedRes
      val projStrategy: ProjectionStrategy = settings.projectionStrategyBuilder.build(settings, rnd, modDF)
      val randomTree = Utils.timed(s"Build tree ${i}", {buildTree(i, rnd, settings, modDF, bucketCollector, splitStrategy, projStrategy, logger)}).result
      randomTrees(i) = RandomTreeNodeRoot(absProjV, randomTree)
    }

    new RandomTrees(splitStrategy, dataFrameView.rowStoredView.getColumnHeader, bucketCollector.build(dataFrameView.getAllLabels()), randomTrees)
  }

}

