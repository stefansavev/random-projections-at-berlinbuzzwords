package com.stefansavev.randomprojections.implementation

import com.stefansavev.randomprojections.datarepr.sparse.SparseVector

object OptimizedHadamard{
  def multiplyIntoHelper(from: Int, input: Array[Double], output: Array[Double]): Unit = {
    var a = input(from)
    var b = input(from + 1)
    output(from) = a + b
    output(from + 1) = a - b

    a = input(from + 2)
    b = input(from + 3)
    output(from + 2) = a + b
    output(from + 3) = a - b

    a = output(from)
    b = output(from + 2)
    output(from) = a + b
    output(from + 2) = a - b

    a = output(from + 1)
    b = output(from + 3)

    output(from + 1) = a + b
    output(from + 3) = a - b
  }

  def mix2(j1: Int, j2: Int, output: Array[Double]): Unit = {
    val a = output(j1)
    val b = output(j2)
    output(j1) = a + b
    output(j2) = a - b
  }

  def mixOld(output: Array[Double]): Unit = {
    val mid = 4
    var j1 = 0
    var j2 = mid
    while(j1 < mid){
      mix2(j1, j2, output)
      /*
      val a = output(j1)
      val b = output(j2)
      output(j1) = a + b
      output(j2) = a - b
      */
      j1 += 1
      j2 += 1
    }
  }

  def mix(output: Array[Double]): Unit = {
    /*
    mix2(0, 4, output)
    mix2(1, 5, output)
    mix2(2, 6, output)
    mix2(3, 7, output)
    */
    val a1 = output(0)
    val a2 = output(1)
    val a3 = output(2)
    val a4 = output(3)

    val a5 = output(4)
    val a6 = output(5)
    val a7 = output(6)
    val a8 = output(7)

    output(0) = a1 + a5
    output(4) = a1 - a5
    output(1) = a2 + a6
    output(5) = a2 - a6
    output(2) = a3 + a7
    output(6) = a3 - a7
    output(3) = a4 + a8
    output(7) = a4 - a8
  }

  def multiplyInto(dim: Int, input: Array[Double], output: Array[Double]): Unit = {
    multiplyIntoHelper(0, input, output)
    multiplyIntoHelper(4, input, output)
    mix(output)
  }

}
object HadamardUtils {
  //assume k is a power of 2
  //TODO: make it work without k being a power of 2
  val eps = 0.0001

  def recurse(from: Int, to: Int, input: Array[Double], output: Array[Double]): Unit = {
    if (to - from == 1){
      output(from) = input(from)
    }
    else if (to - from == 2){
      val a = input(from)
      val b = input(from + 1)
      output(from) = a + b
      output(from + 1) = a - b
    }
    else if (to - from == 4){
      var a = input(from)
      var b = input(from + 1)
      output(from) = a + b
      output(from + 1) = a - b

      a = input(from + 2)
      b = input(from + 3)
      output(from + 2) = a + b
      output(from + 3) = a - b

      a = output(from)
      b = output(from + 2)
      output(from) = a + b
      output(from + 2) = a - b

      a = output(from + 1)
      b = output(from + 3)

      output(from + 1) = a + b
      output(from + 3) = a - b
    }
    else{
      val mid = from + (to - from)/2
      recurse(from, mid, input, output)
      recurse(mid, to, input, output)
      var j1 = from
      var j2 = mid
      while(j1 < mid){
        val a = output(j1)
        val b = output(j2)
        output(j1) = a + b
        output(j2) = a - b
        j1 += 1
        j2 += 1
      }
    }
  }

  object MaxAbsValue{
    //in some cases we put normalized data, but not in query time
    val V: Double = 0.0001 //need to add test cases because for large V this method does not work //0.01
  }
  def argAbsValueMax(dim: Int, values: Array[Double]): Int = {
    //sometimes the values here come normalized and sometimes not, need to fix that
    var i = 0
    var maxAbsValue = MaxAbsValue.V // 0.001 //try channging this after
    var sign = 1
    var maxIdx = 2*dim //last cell is reserved for empty
    while(i < dim){ //forcing split into two
      val v = values(i)
      if (v > maxAbsValue){
        maxAbsValue = v
        sign = 1
        maxIdx = i
      }
      else if (-v > maxAbsValue){
        maxAbsValue = -v
        sign = -1
        maxIdx = i
      }
      i += 1
    }
    //println("max abs. value " + (maxAbsValue, maxIdx) )
    if (maxIdx != 2*dim){
      if (sign > 0) maxIdx else maxIdx + dim
    }
    else{
      maxIdx
    }
  }

  def constrainedArgAbsValueMax(dim: Int, values: Array[Double], availableData: Array[RandomTree]): Int = {
    //sometimes the values here come normalized and sometimes not, need to fix that
    var i = 0
    var maxAbsValue = MaxAbsValue.V // 0.001 //try channging this after
    var sign = 1
    var maxIdx = 2*dim //last cell is reserved for empty
    while(i < dim){ //forcing split into two
      val v = values(i)
      if (v > maxAbsValue && availableData(i) != null ){
        maxAbsValue = v
        sign = 1
        maxIdx = i
      }
      else if (-v > maxAbsValue && availableData(i + dim) != null){
        maxAbsValue = -v
        sign = -1
        maxIdx = i
      }
      i += 1
    }
    //println("max abs. value " + (maxAbsValue, maxIdx) )
    if (maxIdx != 2*dim){
      if (sign > 0) maxIdx else maxIdx + dim
    }
    else{
      maxIdx
    }
  }

  def getAbsValue(dim: Int, values: Array[Double], prevBucketIndex: Int): Double = {
    if (prevBucketIndex >= 2*dim){
      0.0
    }
    else {
      val prevAbsMax = if (prevBucketIndex < dim) values(prevBucketIndex) else -values(prevBucketIndex - dim)
      prevAbsMax
    }
  }

  def nextArgAbsMax(dim: Int, values: Array[Double], prevBucketIndex: Int): Int = {
    val prevAbsMax = getAbsValue(dim, values, prevBucketIndex) //if (prevBucketIndex < values.length) values(prevBucketIndex) else -values(prevBucketIndex - values.length)
    var i = 0
    var maxAbsValue = MaxAbsValue.V //0.001
    var sign = 1
    var maxIdx =  2*dim
    while(i < dim){ //forcing split into two
      val v = values(i)
      if (v > maxAbsValue && v < prevAbsMax){
        maxAbsValue = v
        sign = 1
        maxIdx = i
      }
      else if (-v > maxAbsValue && -v < prevAbsMax){
        maxAbsValue = -v
        sign = -1
        maxIdx = i
      }
      i += 1
    }
    if (maxIdx != -1){
      if (sign > 0) maxIdx else maxIdx + dim
    }
    else{
      maxIdx
    }
  }

  def largestPowerOf2(k: Int): Int = {
    var j = 0
    var i = 1
    while(i <= k){
      j = i
      i = i + i
    }
    j
  }

  def isPowerOf2(k: Int): Boolean = {
    largestPowerOf2(k) == k
  }

  def multiplyInto(input: Array[Double], output: Array[Double]): Unit = {
    val k = largestPowerOf2(input.length)
    var i = 0
    while(i < output.length){
      output(i) = 0.0
      i += 1
    }
    recurse(0, k, input, output)
  }

  def multiplyInto(dim: Int, input: Array[Double], output: Array[Double]): Unit = {
    val k = dim
    var i = 0
    while(i < dim){
      output(i) = 0.0
      i += 1
    }
    recurse(0, k, input, output)
  }

  def normalizeOutput(dim: Int, output: Array[Double]): Unit = {
    val norm = Math.sqrt(dim)
    var i = 0
    while(i < dim){
      output(i) /= norm
      i += 1
    }
  }

  def roundUp(dim: Int): Int = {
    val powOf2 = largestPowerOf2(dim)
    val k = if (powOf2 == dim) dim else 2*powOf2
    k
  }

  def roundDown(dim: Int): Int = {
    largestPowerOf2(dim)
  }

  def computeHadamardFeatures(signs: SparseVector, query: Array[Double], input: Array[Double], output: Array[Double]): Unit = {
    val dim = signs.dim

    //TODO: move to function (significant overlap  with code in Signatures)
    var j = 0
    while(j < input.length){
      input(j)= 0.0
      j += 1
    }

    j = 0
    while(j < signs.ids.length){
      val index = signs.ids(j)
      val b = signs.values(j)
      val a = query(index)
      input(j) = a * b
      j += 1
    }

    HadamardUtils.multiplyInto(input.length, input, output)
  }

}

