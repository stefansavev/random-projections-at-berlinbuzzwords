package com.stefansavev.randomprojections.tuning

import java.util.Random

case class SpeedSimulatorParams(numberOfQueries: Int,
  numberOfTrees: Int,
  requiredPointsPerTree: Int,
  deviationOfRequiredPointsPerTree: Int = 0){
}

object SpeedSimulator {
  val rnd = new Random(11144)

  def testPoint(p: Int, params: SpeedSimulatorParams, buffer: Array[Int]): Int = {
    val rnd = this.rnd
    val len = buffer.length
    var i = 0
    val numTrees = params.numberOfTrees
    val numPointsPerTree = params.requiredPointsPerTree
    val offset = rnd.nextInt(len)
    val stride = offset/params.requiredPointsPerTree  + rnd.nextInt(50)
    var totalOperations = 0
    while(i < numTrees){
      var j = 0
      while(j < numPointsPerTree){
        val k = (offset + stride*j + i) % len //some random formula
        buffer(k) += 1
        j += 1
      }
      i += 1
      totalOperations += 1
    }
    totalOperations
  }

  def simulate(params: SpeedSimulatorParams): Unit = {
    val numQueries = params.numberOfQueries
    val bufflen = params.numberOfQueries
    val buffer = Array.ofDim[Int](bufflen)
    val start = System.currentTimeMillis()
    var i = 0
    while(i < numQueries){
      var j = 0
      while(j < bufflen){
        buffer(j) = 0
        j += 1
      }
      if (i % 5000 == 0){
        println(".")
      }
      testPoint(i, params, buffer)
      i += 1
    }
    val result = System.currentTimeMillis() - start
    println("Simulation in secs: " + result/1000.0 + "    ; per point in ms " + result.toDouble/numQueries.toDouble)
  }

  def main (args: Array[String]): Unit = {
    val params = SpeedSimulatorParams(numberOfQueries = 42000, numberOfTrees = 10, requiredPointsPerTree = 1200)
    simulate(params)
  }
}
