package com.stefansavev.randomprojections.datarepr.sparse

import com.stefansavev.randomprojections.utils.Utils

class SparseVector(val dim: Integer, val ids: Array[Int], val values: Array[Double])

class BinarySparseVector8(val ids: Array[Byte]){

  def toSparseVector(norm: Double = 1.0): SparseVector = {
    val outputIds = Array.ofDim[Int](8)
    val outputValues = Array.ofDim[Double](8)
    var i = 0
    while(i < ids.length){
      val id = ids(i)
      if (id < 0){
        outputIds(i) = -id - 1
        outputValues(i) = -norm
      }
      else{
        outputIds(i) = (id - 1)
        outputValues(i) = norm
      }
      i += 1
    }
    new SparseVector(8, outputIds, outputValues)
  }

  def toNormalizedSparseVector(): SparseVector = {
    toSparseVector(norm = BinarySparseVector8.norm)
  }

  def multiplyVectorComponentWise(queryVec: Array[Double], output: Array[Double]): Unit = {
    multiplyVectorComponentWiseReturnSumOfSquares(queryVec, output)
  }

  def multiplyVectorComponentWiseReturnSumOfSquares(queryVec: Array[Double], output: Array[Double]): Double = {
    val norm = BinarySparseVector8.norm
    var i = 0
    var j = 0

    var sum = 0.0
    while(i < ids.length){
      val id = ids(i)

      val value = if (id > 0){
        queryVec(id - 1)*norm
      }
      else{
        - queryVec(-id - 1)*norm
      }

      output(j) = value
      sum += value*value
      j += 1
      i += 1
    }
    sum
  }
}

object BinarySparseVector8{
  val dim = 8
  val norm = 1.0/Math.sqrt(8.0)

  def fromSparseVec(vec: SparseVector): BinarySparseVector8 = {
    if (vec.dim != 8 && vec.ids.length != 8){
      Utils.internalError()
    }

    val ids = Array.ofDim[Byte](8)
    var i = 0
    while(i < 8){
      val originalId = vec.ids(i)
      if (originalId < 0 || originalId > 100){
        Utils.internalError()
      }
      val id = if (vec.values(i) >= 0.0) (originalId + 1).toByte else (- (originalId + 1)).toByte
      ids(i) = id
      i += 1
    }
    new BinarySparseVector8(ids)
  }
}