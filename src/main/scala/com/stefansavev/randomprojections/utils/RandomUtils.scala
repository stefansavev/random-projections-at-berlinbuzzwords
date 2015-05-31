package com.stefansavev.randomprojections.utils

import java.util.Random

object RandomUtils {
  def shuffleInts(rnd: Random, arr: Array[Int]): Array[Int] = {
    val values = arr.map(v => (v, rnd.nextDouble())).sortBy(_._2).map(_._1)
    values
  }

  def shuffleDoubles(rnd: Random, arr: Array[Double]): Array[Double] = {
    val values = arr.map(v => (v, rnd.nextDouble())).sortBy(_._2).map(_._1)
    values
  }
}
