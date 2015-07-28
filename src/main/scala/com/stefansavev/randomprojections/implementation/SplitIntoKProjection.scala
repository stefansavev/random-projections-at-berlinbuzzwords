package com.stefansavev.randomprojections.implementation

class SplitIntoKProjection {

}

import java.util.Random

import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.utils.RandomUtils

import scala.collection.mutable.ArrayBuffer

case class SplitIntoKProjectionStrategy(rnd: Random, numCols: Int, k: Int) extends ProjectionStrategy{

  def chooseKPoints(k: Int, pointIds: Array[Int], view: DataFrameView): Array[Int] = {
    RandomUtils.shuffleInts(rnd, pointIds).take(k)
  }

  def chooseKDimensions(k: Int): Array[Int] = {
    val columns = Array.range(0, numCols)
    RandomUtils.shuffleInts(rnd, columns).take(k).sorted
  }

  def generateRandomVector(columnIds: Array[Int]): SparseVector = {
    val signs = columnIds.map(_ => (if (rnd.nextDouble() >= 0.5) 1.0 else -1.0))

    var sum = 0.0
    var i = 0
    while(i < signs.length){
      val v = signs(i)
      sum += v*v
      i += 1
    }
    sum = Math.sqrt(sum)

    i = 0
    while(i < signs.length){
      signs(i) /= sum
      i += 1
    }

    val sparseVec = new SparseVector(numCols, columnIds,signs)
    sparseVec
  }

  def generateKRandomVectors(num: Int, columnIds: Array[Int]): Array[SparseVector] = {
      val buff = new ArrayBuffer[SparseVector]()
      for(i <- 0 until num){
        buff += generateRandomVector(columnIds)
      }
      buff.toArray
  }

  def nextRandomProjection(depth: Int, view: DataFrameView): AbstractProjectionVector = {
    val useK = HadamardUtils.largestPowerOf2(k)
    val chosenDim = chooseKDimensions(useK)
    val randomVector = generateRandomVector(chosenDim)
    val proj = new HadamardProjectionVector(randomVector)
    proj
  }
}

case class SplitIntoKProjectionSettings(k: Int)

class SplitIntoKProjectionBuilder(builderSettings: SplitIntoKProjectionSettings) extends ProjectionStrategyBuilder{
  type T = SplitIntoKProjectionStrategy
  val splitStrategy: DatasetSplitStrategy = new HadamardProjectionSplitStrategy()

  def build(settings: IndexSettings, rnd: Random, dataFrameView:DataFrameView): T = SplitIntoKProjectionStrategy(rnd, dataFrameView.numCols, builderSettings.k)
  def datasetSplitStrategy: DatasetSplitStrategy = splitStrategy
}