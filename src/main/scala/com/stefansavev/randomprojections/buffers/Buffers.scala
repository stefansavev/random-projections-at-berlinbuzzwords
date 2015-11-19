package com.stefansavev.randomprojections.buffers

class IntArrayBuffer(preaccloc: Int = 16){
  var array = Array.ofDim[Int](Math.max(preaccloc, 4))
  var current = 0

  def size = current

  def ++=(values: Array[Int]): Unit = {
    if (current + values.length > array.length){
      var newSize = array.length
      while(current + values.length > newSize){
        newSize *= 2
      }
      val doubleArray = Array.ofDim[Int](newSize)
      Array.copy(array, 0, doubleArray, 0, array.length)
      array = doubleArray
    }

    System.arraycopy(values, 0, array, current, values.length)
    current += values.length
  }

  def apply(idx: Int): Int = {
    array(idx)
  }

  def +=(value: Int): Unit = {
    if (current >= array.length){
      val intArray = Array.ofDim[Int](2*array.length)
      Array.copy(array, 0, intArray, 0, array.length)
      array = intArray
    }
    array(current) = value
    current += 1
  }

  def toArray(): Array[Int] = {
    val result = Array.ofDim[Int](current)
    Array.copy(array, 0, result, 0, current)
    result
  }

  def set(index: Int, value: Int): Unit = {
    array(index) = value
  }

  def clear(): Unit = {
    array = Array.ofDim[Int](16)
  }

  def removeLast(): Int = {
    current -= 1
    array(current)
  }
}

class DoubleArrayBuffer(preaccloc: Int = 16){
  var array = Array.ofDim[Double](Math.max(preaccloc, 4))
  var current = 0

  def size = current

  def ++=(values: Array[Double]): Unit = {
    if (current + values.length > array.length){
      var newSize = array.length
      while(current + values.length > newSize){
        newSize *= 2
      }
      val doubleArray = Array.ofDim[Double](newSize)
      Array.copy(array, 0, doubleArray, 0, array.length)
      array = doubleArray
    }

    System.arraycopy(values, 0, array, current, values.length)
    current += values.length
  }

  def +=(value: Double): Unit = {
    if (current >= array.length){
      val doubleArray = Array.ofDim[Double](2*array.length)
      Array.copy(array, 0, doubleArray, 0, array.length)
      array = doubleArray
    }
    array(current) = value
    current += 1
  }

  def toArray(): Array[Double] = {
    val result = Array.ofDim[Double](current)
    Array.copy(array, 0, result, 0, current)
    result
  }

  def clear(): Unit = {
    array = Array.ofDim[Double](16)
  }
}



