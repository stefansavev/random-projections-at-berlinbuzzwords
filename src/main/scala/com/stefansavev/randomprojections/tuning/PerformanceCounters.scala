package com.stefansavev.randomprojections.tuning

import java.text.DecimalFormat

import com.stefansavev.randomprojections.utils.OnlineVariance1

class SimpleStatistics {
  var total = 0.0
  var count = 0.0
  var minValue = Int.MaxValue
  var maxValue = Int.MinValue

  def post(value: Int): Unit = {
    total += value
    count += 1.0
    minValue = Math.min(minValue, value)
    maxValue = Math.max(maxValue, value)
  }

  def getMean(): Double = {
    total / count
  }
}

object PerformanceCounters {

  var leafCountStatistics = new SimpleStatistics()
  var pruningThreshold = new SimpleStatistics()
  var nonPrunedPointsStats = new SimpleStatistics()
  var totalNonLeafs = 0.0
  var numQueries = 0
  var exploredNodes = 0.0
  var exploredBuckets = 0.0
  var touchedPointsDuringSearchCount = 0.0
  var thresholdedPointsDuringSearchCount = 0.0
  var sortedPointsDuringSearchCount = 0.0
  var evaluatedPointsDuringSearchCount = 0.0
  var expectedPointsInBucketsCount = 0.0
  var numTreesCount = 0.0
  var numberOfRequiredPointsPerTreeCount = 0.0
  val timePerQueryInMs = new OnlineVariance1()
  var timePerQueryMax: Long = 0L

  def numTrees(count: Int): Unit = {
    numTreesCount += count
  }

  def processedLeaf(leafCount: Int, leafDepth: Int): Unit = {
    leafCountStatistics.post(leafCount)
  }

  def processNonLeaf(): Unit = {
    totalNonLeafs += 1.0
  }

  def exploreNode(): Unit = {
    exploredNodes += 1.0
  }

  def processQuery(timeInMs: Long): Unit = {
    numQueries += 1
    timePerQueryInMs.processPoint(timeInMs)
    timePerQueryMax = timePerQueryMax max timeInMs
  }

  def exploreBuckets(bucketCountPerQuery: Int): Unit = {
    exploredBuckets += bucketCountPerQuery
  }

  def expectedPointsInBuckets(count: Int): Unit = {
    expectedPointsInBucketsCount += count
  }

  def touchedPointsDuringSearch(count: Int): Unit = {
    touchedPointsDuringSearchCount += count.toDouble
  }

  def thresholdedPointsDuringSearch(count: Int): Unit = {
    thresholdedPointsDuringSearchCount += count.toDouble
  }

  def sortedPointsDuringSearch(count: Int): Unit = {
    sortedPointsDuringSearchCount += count.toDouble
  }

  def evaluatedPointsDuringSearch(count: Int): Unit = {
    evaluatedPointsDuringSearchCount += count.toDouble
  }

  def numberOfRequiredPointsPerTree(count: Int): Unit = {
    numberOfRequiredPointsPerTreeCount += count
  }

  def pruningThreshold(count: Int): Unit = {
    pruningThreshold.post(count)
  }

  def nonPrunedPoints(count: Int): Unit = {
    nonPrunedPointsStats.post(count)
  }

  def report(): Unit = {
    val df = new DecimalFormat("#0.00")
    def customFormat(value: Double): String = {
      df.format(value)
    }

    val numTrees = numTreesCount / numQueries.toDouble
    val avgLeavesPerQueryPerTree = leafCountStatistics.count / (numQueries.toDouble * numTrees)
    val numberOfRequiredPointsPerTree = numberOfRequiredPointsPerTreeCount / numQueries.toDouble
    println("Performance counters report")
    println("---------------------------------------")
    println("Total queries:                " + numQueries)
    println("Number of trees:              " + numTreesCount / numQueries.toDouble)
    println("Required points per tree:     " + numberOfRequiredPointsPerTree)

    println("Avg. leaves per query per tree:        " + customFormat(avgLeavesPerQueryPerTree))
    println("Min. leaf size:               " + customFormat(leafCountStatistics.minValue))
    println("Max. leaf size:               " + customFormat(leafCountStatistics.maxValue))
    println("Avg. leaf size:               " + customFormat(leafCountStatistics.getMean()) + ";  should be = required points per tree/avg. leaves per tree = " + customFormat(numberOfRequiredPointsPerTree / avgLeavesPerQueryPerTree))
    println("Considered tree nodes per query:         " + customFormat(totalNonLeafs / numQueries.toDouble))
    println("Explored nodes per query:     " + customFormat(exploredNodes / numQueries.toDouble))
    println("Explored buckets:             " + customFormat(exploredBuckets / numQueries.toDouble) + "; should be = avg leaves per query * num trees = " + customFormat(avgLeavesPerQueryPerTree * numTrees))
    println("Expected points in buckets:   " + customFormat(expectedPointsInBucketsCount / numQueries.toDouble) + "; should be = num trees * required points per tree = " + customFormat(numTrees * numberOfRequiredPointsPerTree))
    println("Search: Touched points:       " + customFormat(touchedPointsDuringSearchCount / numQueries.toDouble))
    println("Search: Thresholded points:   " + customFormat(thresholdedPointsDuringSearchCount / numQueries.toDouble))
    println("Search: Sorted points:        " + customFormat(sortedPointsDuringSearchCount / numQueries.toDouble))
    println("Search: Evaluated points:     " + customFormat(evaluatedPointsDuringSearchCount / numQueries.toDouble))
    println("Search-Pruning Threshold mean: " + customFormat(pruningThreshold.getMean()))
    println("Search-Pruning Threshold min: " + pruningThreshold.minValue)
    println("Search-Pruning Threshold max: " + pruningThreshold.maxValue)
    println("Search-Non pruned points mean: " + customFormat(nonPrunedPointsStats.getMean()))
    println("Search-Non pruned points min: " + nonPrunedPointsStats.minValue)
    println("Search-Non pruned points max: " + nonPrunedPointsStats.maxValue)
    val (meanTimePerQuery, varPerQuery) = timePerQueryInMs.getMeanAndVar()
    println("Average time per query (mean, std) in ms: " + customFormat(meanTimePerQuery) + "; " + customFormat(Math.sqrt(varPerQuery)))
    println("Max time per query in ms: " + customFormat(timePerQueryMax))
    println("---------------------------------------")
  }

  def initialize(): Unit = {
    leafCountStatistics = new SimpleStatistics()
    totalNonLeafs = 0.0
    numQueries = 0
  }
}
