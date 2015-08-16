package com.stefansavev.randomprojections.utils

import java.util.Random

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
}
