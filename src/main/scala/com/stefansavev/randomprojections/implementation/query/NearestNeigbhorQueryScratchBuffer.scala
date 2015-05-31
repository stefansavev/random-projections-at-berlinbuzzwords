package com.stefansavev.randomprojections.implementation.query

class NearestNeigbhorQueryScratchBuffer(val numRows: Int, val numCols: Int, val usesPointWeights: Boolean){
  val pointCounts = Array.ofDim[Int](numRows)
  val pointsWeights = if (usesPointWeights) Array.ofDim[Double](numRows) else null

  val tmpInput = Array.ofDim[Double](numCols)
  val tmpOutput = Array.ofDim[Double](numCols)
}
