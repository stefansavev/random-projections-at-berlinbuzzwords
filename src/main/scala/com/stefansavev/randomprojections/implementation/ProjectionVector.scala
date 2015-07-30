package com.stefansavev.randomprojections.implementation

import java.util.Random

import com.stefansavev.randomprojections.buffers.{DoubleArrayBuffer, IntArrayBuffer}
import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer

trait AbstractProjectionVector{
  def getProjectionTypeName: String
}

case class HadamardProjectionVector(signs: SparseVector) extends AbstractProjectionVector{
  def getProjectionTypeName: String  = "hadamardProjectionVector"
}

trait ProjectionStrategy{
  def nextRandomProjection(depth: Int, view: DataFrameView, prevProjection: AbstractProjectionVector): AbstractProjectionVector
}

trait ProjectionStrategyBuilder{
  type T <: ProjectionStrategy
  def build(settings: IndexSettings, rnd: Random, dataFrameView: DataFrameView): T
  def datasetSplitStrategy: DatasetSplitStrategy
}

class SearchBucketsResult(numFeatures: Int, scratch: NearestNeigbhorQueryScratchBuffer){
  var depth: Int = 1
  def tmpInput: Array[Double] = scratch.tmpInput
  def tmpOutput: Array[Double] = scratch.tmpOutput
  val bucketIndexBuffer: IntArrayBuffer = new IntArrayBuffer()
  val bucketScoreBuffer: DoubleArrayBuffer = new DoubleArrayBuffer()
}