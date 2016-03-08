package com.stefansavev.randomprojections.implementation.indexing

import java.util.Random
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.interface.BucketCollector
import com.stefansavev.randomprojections.datarepr.dense._
import com.stefansavev.randomprojections.dimensionalityreduction.interface.{NoDimensionalityReductionTransform, DimensionalityReduction}
import com.stefansavev.randomprojections.dimensionalityreduction.svd._
import com.stefansavev.randomprojections.utils.Utils
import com.typesafe.scalalogging.StrictLogging

trait Point2LeavesMap{
  def getTreeLeaves(pointId: Int): Array[(Int, Int)]
}

class Point2LeavesMapImpl(map: scala.collection.mutable.HashMap[Int, Array[(Int, Int)]]) extends Point2LeavesMap{
  def getTreeLeaves(pointId: Int): Array[(Int, Int)] = {
    map(pointId)
  }
}

object IndexBuilder extends StrictLogging{
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
                 indexCounters: IndexCounters): RandomTree = {

    val counter = new Counter()
    def loop(dataFrameView: DataFrameView, prevProjection: AbstractProjectionVector, depth: Int, noProgressCount: Int): RandomTree = {
      val hasReachedDepth = settings.maxDepth match {case None => false; case Some(v) => depth >= v}
      if (dataFrameView.numRows <= 0){
        EmptyLeaf //too little data
      }
      else if (hasReachedDepth || (dataFrameView.numRows < settings.maxPntsPerBucket || noProgressCount >= 10)) {
        if (noProgressCount >= 10){
          val numPointsInBucket = dataFrameView.getUnderlyingIndexes().size
          logger.info(s"No progress in >= 10 iterations. Saving ${numPointsInBucket} datapoints to the bucket")
        }
        collector.collectPoints(dataFrameView.getUnderlyingIndexes())
      }
      else {
        val nodeId = counter.next() //for debugging purposes
        val projectionVector = projStrategy.nextRandomProjection(depth, dataFrameView, prevProjection)
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
          loop(dataFrameView, prevProjection, depth, noProgressCount + 1)
        }
        else {
          val children = Array.ofDim[RandomTree](splits.size)
          var count = 0
          var i = 0
          while (i < splits.length) {
            val slice = splits(i)
            if (slice.numRows > 0){
              val subTree = loop(slice, projectionVector, depth + 1, 0)
              children(i) = subTree
              count += subTree.getCount
            }

            i += 1
          }

          RandomTreeNode(nodeId, projectionVector, count, means, children)
        }
      }
    }
    loop(inputDataView, null, 0, 0)
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
    val header = ColumnHeaderBuilder.build(oldHeader.labelName, newF, false, dataFrame.rowStoredView.getBuilderType)
    val builder = DenseRowStoredMatrixViewBuilderFactory.create(header)
    val absprojVec = projSt.nextRandomProjection(0, dataFrame, null)
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

  def modifyInPlace(dataFrame: DataFrameView, projSt: ProjectionStrategy): (AbstractProjectionVector, DataFrameView) = {
    val absprojVec = projSt.nextRandomProjection(0, dataFrame, null)
    val signs = absprojVec.asInstanceOf[HadamardProjectionVector].signs
    val vec = Array.ofDim[Double](dataFrame.numCols)
    val input = Array.ofDim[Double](signs.ids.length)
    val output = Array.ofDim[Double](signs.ids.length)
    val newIds = Array.range(0, dataFrame.numCols)
    val denseRowStoredMatrix = dataFrame.rowStoredView.asInstanceOf[DenseRowStoredMatrixView]
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
      HadamardUtils.multiplyInto(dataFrame.numCols, input, output)
      denseRowStoredMatrix.setRow(i, output)
      i += 1
    }

    val indexes = dataFrame.indexes
    (absprojVec, dataFrame)
  }

  private def requiresStorageOfOriginalDataset(evaluator: ReportingDistanceEvaluator): Boolean = {
    val req = evaluator.dataSetSerializationRequirement
    req match {
      case NoDatasetSerializationRequriement => false
      case OriginalDatasetSerializationRequirement(_) => true
    }
  }

  def toEfficientlyStoredTree(tree: RandomTree): RandomTree = {
    RandomTree2EfficientlyStoredTreeConverter.toEfficientlyStoredTree(tree)
  }

  def computeSignatures(rnd: Random, numSig: Int, dataFrameView: DataFrameView): (SignatureVectors, PointSignatures) = {
    val onlineSigVecs = new OnlineSignatureVectors(rnd, numSig, dataFrameView.numCols)
    var i = 0
    while(i < dataFrameView.numRows){
      onlineSigVecs.pass(dataFrameView.getPointAsDenseVector(i))
      i += 1
    }
    val (signatureVecs, signatures) = onlineSigVecs.buildPointSignatures()
    (signatureVecs, signatures)
  }

  def buildWithSVDAndRandomRotation(treesBackingDir: String, k: Int, settings: IndexSettings, dataFrameView: DataFrameView,
                                    precomputedSVDTransform: Option[SVDTransform] = None,
                                    precomputedSigVec: Option[(SignatureVectors, PointSignatures)] = None): RandomTrees = {
    val bucketCollector = new BucketCollectorImpl(treesBackingDir, dataFrameView.numRows)
    val rnd = new Random(settings.randomSeed)
    val indexCounters = new IndexCounters()
    val randomTrees = Array.ofDim[RandomTree](settings.numTrees)

    //phase 1: dimensionality reduction

    val svdTransform = precomputedSVDTransform match {
        case None => {
          val svdParams = SVDParams(k, FullDenseSVDLimitedMemory)
          DimensionalityReduction.fit(svdParams, dataFrameView)
        }
        case Some(t) => {
          //we can precompute the transform while adding the documents to the index
          //this is advantageous if the dataset is large and we cannot keep it
          t.reduceToTopK(k)
        }
      }

    val datasetAfterSVD = DimensionalityReduction.transform(svdTransform, dataFrameView)

    //phase 2 place holder
    val newIndexes = PointIndexes(dataFrameView.indexes.indexes)
    val rotatedDatasetPlaceHolder = new DataFrameView(newIndexes, datasetAfterSVD.rowStoredView)

    val splitStrategy = settings.projectionStrategyBuilder.datasetSplitStrategy
    //val (signatureVecs, signatures) = Signatures.computePointSignatures(settings.signatureSize, rnd, dataFrameView)
    val (signatureVecs, signatures) = precomputedSigVec match {
      case None => computeSignatures(rnd, settings.signatureSize, dataFrameView)
      case Some(sv) => sv
    }

    //dataFrameView.setPointSignatures(signatures) //pointSig. from dataFrame will be removed

    val reportingDistanceEvaluator = settings.reportingDistanceEvaluator.build(dataFrameView)

    for(i <- 0 until settings.numTrees){
      //phase 3: run fast trees
      val randomRotation = ProjectionStrategies.splitIntoKRandomProjection(k).build(settings, rnd, datasetAfterSVD)
      val (rotationTransform, rotatedDataFrameView)= modifyDataFrame(rotatedDatasetPlaceHolder, randomRotation)
      //TODO: in principle modify in place can work; the advantage is that we don't allocate memory twice
      //to store the matrix
      //val (rotationTransform, rotatedDataFrameView) = modifyInPlace(datasetAfterSVD, randomRotation)

      val projStrategy: ProjectionStrategy = settings.projectionStrategyBuilder.build(settings, rnd, rotatedDataFrameView)
      val randomTree = Utils.timed(s"Build tree ${i}", {
                                toEfficientlyStoredTree(buildTree(i, rnd, settings, rotatedDataFrameView, bucketCollector, splitStrategy, projStrategy, indexCounters))
                            })(logger)
      randomTrees(i) = RandomTreeNodeRoot(rotationTransform, randomTree.result)
    }

    new RandomTrees(svdTransform, reportingDistanceEvaluator, signatureVecs, splitStrategy, dataFrameView.rowStoredView.getColumnHeader, bucketCollector.build(signatures, dataFrameView.getAllLabels()), randomTrees)
  }

}

