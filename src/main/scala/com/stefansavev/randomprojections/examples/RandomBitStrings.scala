package com.stefansavev.randomprojections.examples

import java.util.Random

import com.stefansavev.randomprojections.datarepr.dense.{ColumnHeaderBuilder, DataFrameView, PointIndexes, RowStoredMatrixViewBuilderFactory}

object RandomBitStrings {
  case class RandomBitSettings(numGroups: Int, numRowsPerGroup: Int, numCols: Int, per1sInPrototype: Double, perNoise: Double)

  def generatePrototype(rnd: Random, dim: Int, perValue: Double): Array[Double] = {
    val arr = Array.ofDim[Double](dim)
    for(i <- 0 until dim){
      arr(i) = -1.0
      if (rnd.nextDouble() < perValue){
        arr(i) = 1.0
      }
    }
    arr
  }

  def corrupt(rnd: Random, input: Array[Double], perNoise: Double): Array[Double] = {
    val arr = Array.ofDim[Double](input.length)
    for(i <- 0 until input.length){
      if (rnd.nextDouble() < perNoise){
        arr(i) = -input(i)
      }
      else{
        arr(i) = input(i)
      }
    }
    arr
  }

  def genRandomData(seed: Int, settings: RandomBitSettings, debug: Boolean, dense:Boolean): DataFrameView = {

    val (numGroups, numRowsPerGroup, numCols: Int, per1sInPrototype: Double, perNoise: Double) =
      (settings.numGroups, settings.numRowsPerGroup, settings.numCols, settings.per1sInPrototype, settings.perNoise)

    val numRows = numGroups * numRowsPerGroup

    val labels = Array.ofDim[Int](numRows)
    val rnd = new Random(seed)
    var i = 0

    val columnNames = Array.range(0, numCols).map((i: Int) => ("feature" + i, i))
    val header = ColumnHeaderBuilder.build("label", columnNames, Array.empty)

    val builder = RowStoredMatrixViewBuilderFactory.createDense(header)

    for(g <- 0 until numGroups){
      val prototype = generatePrototype(rnd, numCols, per1sInPrototype)
      for(r <- 0 until numRowsPerGroup){
        val noisyProt = corrupt(rnd, prototype, perNoise)
        labels(i) = g
        if (i != builder.currentRowId){
          throw new IllegalStateException("Cannot skip rows")
        }
        builder.addRow(g, Array.range(0, numCols), noisyProt)
        i += 1
      }
    }
    val indexes = PointIndexes(Array.range(0, numRows))
    new DataFrameView(indexes, builder.build())
  }

}
