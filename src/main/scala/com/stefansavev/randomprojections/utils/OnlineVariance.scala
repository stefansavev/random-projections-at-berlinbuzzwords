package com.stefansavev.randomprojections.utils

class OnlineVariance(k: Int) {
  var n = 0.0
  val mean = Array.ofDim[Double](k)

  val M2 = Array.ofDim[Double](k)

  val delta = Array.ofDim[Double](k)

  def processPoint(point: Array[Double]): Unit = {
    val x = point
    n = n + 1.0
    var j = 0
    while (j < k) {
      delta(j) = x(j) - mean(j)
      mean(j) = mean(j) + delta(j) / n
      M2(j) = M2(j) + delta(j) * (x(j) - mean(j))
      j += 1
    }
  }

  def getMeanAndVar(): (Array[Double], Array[Double]) = {
    val variance = Array.ofDim[Double](k)
    var j = 0
    while (j < k) {
      variance(j) = M2(j) / (n - 1.0)
      j += 1
    }
    (mean, variance)
  }
}

class OnlineVariance1 {
  val onlineVariance = new OnlineVariance(1)
  val buffer = Array.ofDim[Double](1)

  def processPoint(value: Double): Unit = {
    buffer(0) = value
    onlineVariance.processPoint(buffer)
  }

  def getMeanAndVar(): (Double, Double) = {
    val (means, variances) = onlineVariance.getMeanAndVar()
    (means(0), variances(0))
  }
}
