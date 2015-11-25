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


class FloatArrayBuffer(preaccloc: Int = 16){
  var array = Array.ofDim[Float](Math.max(preaccloc, 4))
  var current = 0

  def size = current

  def ++=(values: Array[Float]): Unit = {
    if (current + values.length > array.length){
      var newSize = array.length
      while(current + values.length > newSize){
        newSize *= 2
      }
      val doubleArray = Array.ofDim[Float](newSize)
      Array.copy(array, 0, doubleArray, 0, array.length)
      array = doubleArray
    }

    System.arraycopy(values, 0, array, current, values.length)
    current += values.length
  }

  def +=(value: Float): Unit = {
    if (current >= array.length){
      val doubleArray = Array.ofDim[Float](2*array.length)
      Array.copy(array, 0, doubleArray, 0, array.length)
      array = doubleArray
    }
    array(current) = value
    current += 1
  }

  def toArray(): Array[Float] = {
    val result = Array.ofDim[Float](current)
    Array.copy(array, 0, result, 0, current)
    result
  }

  def clear(): Unit = {
    array = Array.ofDim[Float](16)
  }
}

class ShortArrayBuffer(preaccloc: Int = 16){
  var array = Array.ofDim[Short](Math.max(preaccloc, 4))
  var current = 0

  def size = current

  def ++=(values: Array[Short]): Unit = {
    if (current + values.length > array.length){
      var newSize = array.length
      while(current + values.length > newSize){
        newSize *= 2
      }
      val doubleArray = Array.ofDim[Short](newSize)
      Array.copy(array, 0, doubleArray, 0, array.length)
      array = doubleArray
    }

    System.arraycopy(values, 0, array, current, values.length)
    current += values.length
  }

  def apply(idx: Int): Int = {
    array(idx)
  }

  def +=(value: Short): Unit = {
    if (current >= array.length){
      val intArray = Array.ofDim[Short](2*array.length)
      Array.copy(array, 0, intArray, 0, array.length)
      array = intArray
    }
    array(current) = value
    current += 1
  }

  def toArray(): Array[Short] = {
    val result = Array.ofDim[Short](current)
    Array.copy(array, 0, result, 0, current)
    result
  }

  def set(index: Int, value: Short): Unit = {
    array(index) = value
  }

  def clear(): Unit = {
    array = Array.ofDim[Short](16)
  }

  def removeLast(): Short = {
    current -= 1
    array(current)
  }
}


class ByteArrayBuffer(preaccloc: Int = 16){
  var array = Array.ofDim[Byte](Math.max(preaccloc, 4))
  var current = 0

  def size = current

  def ++=(values: Array[Byte]): Unit = {
    if (current + values.length > array.length){
      var newSize = array.length
      while(current + values.length > newSize){
        newSize *= 2
      }
      val doubleArray = Array.ofDim[Byte](newSize)
      Array.copy(array, 0, doubleArray, 0, array.length)
      array = doubleArray
    }

    System.arraycopy(values, 0, array, current, values.length)
    current += values.length
  }

  def apply(idx: Int): Int = {
    array(idx)
  }

  def +=(value: Byte): Unit = {
    if (current >= array.length){
      val intArray = Array.ofDim[Byte](2*array.length)
      Array.copy(array, 0, intArray, 0, array.length)
      array = intArray
    }
    array(current) = value
    current += 1
  }

  def toArray(): Array[Byte] = {
    val result = Array.ofDim[Byte](current)
    Array.copy(array, 0, result, 0, current)
    result
  }

  def set(index: Int, value: Byte): Unit = {
    array(index) = value
  }

  def clear(): Unit = {
    array = Array.ofDim[Byte](16)
  }

  def removeLast(): Byte = {
    current -= 1
    array(current)
  }
}



