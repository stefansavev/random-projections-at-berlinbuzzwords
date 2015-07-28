package com.stefansavev.randomprojections.implementation

import com.stefansavev.randomprojections.buffers.IntArrayBuffer
import com.stefansavev.randomprojections.datarepr.dense.{PointIndexes, DataFrameView}
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.tuning.PerformanceCounters

trait DatasetSplitStrategy {
  def splitDataset(inputData: DataFrameView, abstractProjVector: AbstractProjectionVector): (Array[DataFrameView], Array[Double])
  def processNonChildNodeDuringBucketSearch(numberOfRequiredPointsPerTree: Int, enableGreedy: Boolean, pq: BucketSearchPriorityQueue, depth: Int, prevScoresSum: Double, rtn: RandomTreeNode, query: Array[Double], state: SearchBucketsResult): Unit
}

class HadamardProjectionSplitStrategy extends DatasetSplitStrategy{
  def getBucketIndex(tmpInput: Array[Double], tmpOutput: Array[Double]): Int = {
    val dim = tmpInput.length
    var i = 0
    var sum = 0.0
    while(i < dim){
      val p = tmpInput(i)
      tmpInput(i) = p
      sum += p*p
      i += 1
    }

    sum = Math.sqrt(sum + HadamardUtils.eps)
    i = 0
    while(i < dim){
      tmpInput(i) /= sum
      i += 1
    }

    HadamardUtils.multiplyInto(dim, tmpInput, tmpOutput)
    val bucketIndex = HadamardUtils.argAbsValueMax(dim, tmpOutput)
    bucketIndex
  }

  def splitDataset(inputData: DataFrameView, abstractProjVector: AbstractProjectionVector): (Array[DataFrameView], Array[Double])  = {
    val signs: SparseVector = abstractProjVector.asInstanceOf[HadamardProjectionVector].signs
    val dim = signs.ids.length
    val numberOfOutputs = 2*dim + 1
    //TODO:
    val tmpInput = Array.ofDim[Double](dim)
    val tmpOutput = Array.ofDim[Double](dim)

    val pointIndexes = inputData.getUnderlyingIndexes()

    val splits = Array.ofDim[IntArrayBuffer](numberOfOutputs).map(_=> new IntArrayBuffer())

    var i = 0
    while(i < pointIndexes.size){
      val rowId = pointIndexes(i)
      inputData.multiplyRowComponentWiseBySparseVector(rowId, signs, tmpInput)
      val bestIdx = getBucketIndex(tmpInput, tmpOutput)
      splits(bestIdx) += rowId
      i += 1
    }
    val childFrames =  splits.map(buf => inputData.childView(PointIndexes(buf.toArray)))
    val means = Array.ofDim[Double](dim) //TODO: right now the means are not used
    (childFrames, means)
  }

  def processNonChildNodeDuringBucketSearch(numberOfRequiredPointsPerTree: Int, enableGreedy: Boolean, pq: BucketSearchPriorityQueue, depth: Int, prevScoresSum: Double, rtn: RandomTreeNode, query: Array[Double], state: SearchBucketsResult): Unit = {
    val projVector = rtn.projVector
    val sparseVec = projVector.asInstanceOf[HadamardProjectionVector].signs
    val dim = sparseVec.ids.length
    val input = state.tmpInput

    var i = 0
    var sum = 0.0
    while(i < dim){
      val columnId = sparseVec.ids(i)
      val p = sparseVec.values(i)*query(columnId) //- means(i)
      input(i) = p
      sum += p*p
      i += 1
    }

    sum = Math.sqrt(sum + HadamardUtils.eps)
    //TODO: This normalization is not necessary
    i = 0
    while(i < dim){
      input(i) /= sum
      i += 1
    }

    val output = state.tmpOutput
    HadamardUtils.multiplyInto(dim, input, output)
    //greedy

    var requireFullProcessing = true
    if (enableGreedy) {
      val children = rtn.children
      val bestBucketIndex = HadamardUtils.constrainedArgAbsValueMax(dim, output, children)
      val absScore = HadamardUtils.getAbsValue(dim, output, bestBucketIndex)
      val bestChild = children(bestBucketIndex)
      if (bestChild != null && bestChild.getCount >= numberOfRequiredPointsPerTree) {
        val sumScores = (prevScoresSum + absScore)
        pq.add(sumScores, depth + 1, bestChild)
        PerformanceCounters.processNonLeaf()
        requireFullProcessing = false
      }
    }

    if (requireFullProcessing) {
      i = 0
      while (i < dim) {
        val value = output(i)
        val bucketIndex = if (value > 0.0) i else if (value < 0.0) (i + dim) else 2 * dim
        val absScore = Math.abs(value)
        val child = rtn.children(bucketIndex)
        if (child != null) {
          val sumScores = (prevScoresSum + absScore)
          pq.add(sumScores, depth + 1, child)
          PerformanceCounters.processNonLeaf()
        }
        i += 1
      }
    }
  }
}

class DataInformedSplitStrategy extends DatasetSplitStrategy{

  def sum(values: Array[Double]): Double = {
    var i = 0
    var sum = 0.0
    while(i < values.length){
      sum += values(i)
      i += 1
    }
    sum
  }

  def splitDataset(inputData: DataFrameView, abstractProjVector: AbstractProjectionVector): (Array[DataFrameView], Array[Double])  = {
    val signs: SparseVector = abstractProjVector.asInstanceOf[HadamardProjectionVector].signs
    val dim = signs.ids.length
    val numberOfOutputs = 2

    val tmpInput = Array.ofDim[Double](dim)

    val pointIndexes = inputData.getUnderlyingIndexes()

    val splits = Array.ofDim[IntArrayBuffer](numberOfOutputs).map(_=> new IntArrayBuffer())

    var i = 0
    while(i < pointIndexes.size){
      val rowId = pointIndexes(i)
      inputData.multiplyRowComponentWiseBySparseVector(rowId, signs, tmpInput) //TODO: optimize
      val value = sum(tmpInput)
      val bestIdx = if (value >= 0) 0 else 1
      splits(bestIdx) += rowId
      i += 1
    }
    val childFrames =  splits.map(buf => inputData.childView(PointIndexes(buf.toArray)))
    val means = Array.ofDim[Double](0) //TODO: right now the means are not used
    (childFrames, means)
  }

  def processNonChildNodeDuringBucketSearch(numberOfRequiredPointsPerTree: Int, enableGreedy: Boolean, pq: BucketSearchPriorityQueue, depth: Int, prevScoresSum: Double, rtn: RandomTreeNode, query: Array[Double], state: SearchBucketsResult): Unit = {
    val projVector = rtn.projVector
    val sparseVec = projVector.asInstanceOf[HadamardProjectionVector].signs
    val dim = sparseVec.ids.length

    var i = 0
    var dotProd = 0.0
    while(i < dim){
      val columnId = sparseVec.ids(i)
      val p = sparseVec.values(i)*query(columnId)
      dotProd += p
      i += 1
    }

    if (dotProd >= 0){
      val child = rtn.children(0)
      if (child != null) {
        pq.add(dotProd, depth + 1, child)
        PerformanceCounters.processNonLeaf()
      }
      val child2 = rtn.children(1)
      if (child2 != null) {
        pq.add(-dotProd, depth + 1, child2)
        PerformanceCounters.processNonLeaf()
      }
    }
    else{
      val child = rtn.children(1)
      if (child != null) {
        pq.add(-dotProd, depth + 1, child)
        PerformanceCounters.processNonLeaf()
      }
      val child2 = rtn.children(0)
      if (child2 != null) {
        pq.add(dotProd, depth + 1, child2)
        PerformanceCounters.processNonLeaf()
      }
    }
  }
}


