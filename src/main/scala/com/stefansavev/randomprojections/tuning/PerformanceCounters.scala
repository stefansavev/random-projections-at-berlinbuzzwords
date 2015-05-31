package com.stefansavev.randomprojections.tuning

class SimpleStatistics{
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
    total/count
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

  def processQuery(): Unit = {
    numQueries += 1
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
    val numTrees = numTreesCount/numQueries.toDouble
    val avgLeavesPerQueryPerTree = leafCountStatistics.count/(numQueries.toDouble * numTrees)
    val numberOfRequiredPointsPerTree = numberOfRequiredPointsPerTreeCount / numQueries.toDouble
    println("Performance counters report")
    println("---------------------------------------")
    println("Total queries:                " + numQueries)
    println("Number of trees:              " + numTreesCount/numQueries.toDouble)
    println("Required points per tree:     " + numberOfRequiredPointsPerTree)

    println("Avg. leaves per query per tree:        " + avgLeavesPerQueryPerTree)
    println("Min. leaf size:               " + leafCountStatistics.minValue)
    println("Max. leaf size:               " + leafCountStatistics.maxValue)
    println("Avg. leaf size:               " + leafCountStatistics.getMean() + ";  should be = required points per tree/avg. leaves per tree = " + numberOfRequiredPointsPerTree/avgLeavesPerQueryPerTree)
    println("Considered tree nodes per query:         " + totalNonLeafs/numQueries.toDouble)
    println("Explored nodes per query:     " + exploredNodes/numQueries.toDouble)
    println("Explored buckets:             " + exploredBuckets/numQueries.toDouble + "; should be = avg leaves per query * num trees = " + avgLeavesPerQueryPerTree * numTrees)
    println("Expected points in buckets:   " + expectedPointsInBucketsCount/numQueries.toDouble + "; should be = num trees * required points per tree = " + numTrees * numberOfRequiredPointsPerTree)
    println("Search: Touched points:       " + touchedPointsDuringSearchCount/numQueries.toDouble)
    println("Search: Thresholded points:   " + thresholdedPointsDuringSearchCount/numQueries.toDouble)
    println("Search: Sorted points:        " + sortedPointsDuringSearchCount/numQueries.toDouble)
    println("Search: Evaluated points:     " + evaluatedPointsDuringSearchCount/numQueries.toDouble)
    println("Search-Pruning Threshold mean: " + pruningThreshold.getMean())
    println("Search-Pruning Threshold min: " + pruningThreshold.minValue)
    println("Search-Pruning Threshold max: " + pruningThreshold.maxValue)
    println("Search-Non pruned points mean: " + nonPrunedPointsStats.getMean())
    println("Search-Non pruned points min: " + nonPrunedPointsStats.minValue)
    println("Search-Non pruned points max: " + nonPrunedPointsStats.maxValue)
    println("---------------------------------------")
  }

  def initialize(): Unit = {
    leafCountStatistics = new SimpleStatistics()
    totalNonLeafs = 0.0
    numQueries = 0
  }
}
