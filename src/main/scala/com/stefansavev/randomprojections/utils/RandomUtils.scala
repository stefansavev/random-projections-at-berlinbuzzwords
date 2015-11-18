package com.stefansavev.randomprojections.utils

import java.util.Random

import com.stefansavev.randomprojections.buffers.IntArrayBuffer
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector

object RandomUtils {
  def shuffleInts(rnd: Random, arr: Array[Int]): Array[Int] = {
    val values = arr.map(v => (v, rnd.nextDouble())).sortBy(_._2).map(_._1)
    values
  }

  def shuffleDoubles(rnd: Random, arr: Array[Double]): Array[Double] = {
    val values = arr.map(v => (v, rnd.nextDouble())).sortBy(_._2).map(_._1)
    values
  }

  def sign(rnd: Random): Double = {
    if (rnd.nextDouble() > 0.5) 1.0 else -1.0
  }

  def generateRandomVector(rnd: Random, numCols: Int, columnIds: Array[Int]): SparseVector = {
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

  def generateRandomVector(rnd: Random, numCols: Int): SparseVector = {
    generateRandomVector(rnd, numCols, Array.range(0, numCols))
  }

  //TODO: use a version of reservoir sampling together with random shuffle
  def sample(rnd: Random, k: Int, arr: Array[Int]): Array[Int] = {
    def getValue(arr: Array[Int], overWrites: scala.collection.mutable.HashMap[Int, Int], index: Int): Int = {
      if (overWrites.contains(index)){ overWrites(index) } else {arr(index)}
    }
    var currentLength = arr.length
    val buffer = new IntArrayBuffer()
    val overWrites = new scala.collection.mutable.HashMap[Int, Int]()
    var i = 0
    while(i < k && currentLength > 0){
      val nextPos = rnd.nextInt(currentLength)
      val sampledValue = getValue(arr, overWrites, nextPos)
      buffer += sampledValue
      if (nextPos < currentLength - 1) {
        val lastValue = getValue(arr, overWrites, currentLength - 1)
        overWrites += ((nextPos, lastValue))
      }
      currentLength -= 1
      i += 1
    }
    buffer.toArray()
  }
}
