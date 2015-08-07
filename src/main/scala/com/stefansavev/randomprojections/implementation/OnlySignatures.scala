package com.stefansavev.randomprojections.implementation

import java.util.Random
import com.stefansavev.randomprojections.utils.Utils
import com.stefansavev.randomprojections.datarepr.dense.DataFrameView

case class OnlySignaturesStrategy(rnd: Random, numCols: Int) extends ProjectionStrategy{
  def nextRandomProjection(depth: Int, view: DataFrameView, prevProjection: AbstractProjectionVector): AbstractProjectionVector = {
    Utils.internalError() //this should not be called
  }
}

case class OnlySignaturesSettings()

class NoSplitStrategy extends DatasetSplitStrategy{
  def splitDataset(inputData: DataFrameView, abstractProjVector: AbstractProjectionVector): (Array[DataFrameView], Array[Double])  = {
    Utils.internalError()
  }

  def processNonChildNodeDuringBucketSearch(numberOfRequiredPointsPerTree: Int, enableGreedy: Boolean, pq: BucketSearchPriorityQueue, depth: Int, prevScoresSum: Double, rtn: RandomTreeNode, query: Array[Double], state: SearchBucketsResult): Unit = {
    Utils.internalError()
  }

}

class OnlySignaturesBuilder(builderSettings: OnlySignaturesSettings) extends ProjectionStrategyBuilder{
  type T = OnlySignaturesStrategy
  val splitStrategy = new NoSplitStrategy()
  def build(settings: IndexSettings, rnd: Random, dataFrameView:DataFrameView): T = OnlySignaturesStrategy(rnd, dataFrameView.numCols)

  def datasetSplitStrategy: DatasetSplitStrategy = splitStrategy
}
