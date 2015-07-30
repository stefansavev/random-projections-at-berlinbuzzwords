package com.stefansavev.randomprojections.implementation

import java.util.Random
import com.stefansavev.randomprojections.buffers.IntArrayBuffer
import com.stefansavev.randomprojections.datarepr.dense.{PointIndexes, DataFrameView}
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.utils.RandomUtils

class OnlineVariance(k: Int) {
  var n = 0.0
  val mean = Array.ofDim[Double](k)

  val sqrLeft = Array.ofDim[Double](k)
  val cntsLeft = Array.ofDim[Double](k)

  val sqrRight = Array.ofDim[Double](k)
  val cntsRight = Array.ofDim[Double](k)

  val M2 = Array.ofDim[Double](k)

  val delta = Array.ofDim[Double](k)

  def processPoint(point: Array[Double]): Unit = {
    val x = point
    n = n + 1.0
    var j = 0
    while (j < k) {
      delta(j) = x(j) - mean(j)
      mean(j) = mean(j) + delta(j) / n
      M2(j) = M2(j) + delta(j) * (x(j) - mean(j))

      if (x(j) > 0){
        sqrLeft(j) += x(j)*x(j)
        cntsLeft(j) += 1
      }
      else{
        sqrRight(j) += x(j)*x(j)
        cntsRight(j) += 1
      }

      j += 1
    }
  }

  def getMeanAndVar(): (Array[Double], Array[Double]) = {
    var j = 0
    while (j < k) {
      M2(j) = M2(j) / (n - 1.0) //M2 is now the variance
      j += 1
    }
    (mean, M2)
  }

  def diffSqr(): Array[Double] = {
    var j = 0
    val v = Array.ofDim[Double](k)
    while (j < k) {
      val a = sqrLeft(j)/(cntsLeft(j) + 0.5)
      val b = sqrRight(j)/(cntsRight(j) + 0.5)
      v(j) = Math.sqrt(a*b)
      j += 1
    }
    v
  }
}

case class DataInformedProjectionStrategy(rnd: Random, numCols: Int) extends ProjectionStrategy{
  def nextRandomProjection(depth: Int, view: DataFrameView, prevProjVector: AbstractProjectionVector): AbstractProjectionVector = {
    val indexes = view.indexes.indexes
    val a = rnd.nextInt(indexes.length)
    val b0 = rnd.nextInt(indexes.length - 1)
    val b = if (b0 == a) (indexes.length - 1) else b0

    /*
    val onlineVariance = new OnlineVariance(numCols)
    for(_id <- indexes){
      val p = view.getPointAsDenseVector(_id)
      onlineVariance.processPoint(p)
    }

    val (mean, variance) = onlineVariance.getMeanAndVar()
    */

    val vA = view.getPointAsDenseVector(a)
    val vB = view.getPointAsDenseVector(b)
    var norm = 0.0
    var i = 0
    while(i < vA.length){
      //vA(i) -= vB(i)
      //vA(i) = Math.signum((vA(i) - vB(i)))*Math.abs(rnd.nextGaussian()) * (Math.sqrt(variance(i) + 0.1) )
      vA(i) = (vA(i) - vB(i)) // + rnd.nextGaussian()*Math.sqrt(variance(i))
      norm += vA(i)*vA(i)
      i += 1
    }
    norm = Math.sqrt(norm + 0.001)
    i = 0
    while(i < vA.length){
      vA(i) /= norm
      i += 1
    }
    val randomVector = new SparseVector(numCols, Array.range(0, numCols), vA)
    val proj = new HadamardProjectionVector(randomVector)
    proj
  }
}

case class DataInformedProjectionSettings()

class DataInformedProjectionBuilder(builderSettings: DataInformedProjectionSettings) extends ProjectionStrategyBuilder{
  type T = DataInformedProjectionStrategy
  val splitStrategy: DatasetSplitStrategy = new DataInformedSplitStrategy()

  def build(settings: IndexSettings, rnd: Random, dataFrameView:DataFrameView): T = DataInformedProjectionStrategy(rnd, dataFrameView.numCols)
  def datasetSplitStrategy: DatasetSplitStrategy = splitStrategy
}
