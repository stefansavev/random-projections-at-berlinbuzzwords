package com.stefansavev.randomprojections.implementation.bucketsearch

import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.tuning.PerformanceCounters

case class PriorityQueueBasedBucketSearchSettings(numberOfRequiredPointsPerTree: Int, enableGreedy: Boolean = false) extends BucketSearchSettings

class PriorityQueueBasedBucketSearchStrategy(datasetSplitStrategy: DatasetSplitStrategy, settings: PriorityQueueBasedBucketSearchSettings) extends BucketSearchStrategy {

  def processNonChildNode(datasetSplitStrategy: DatasetSplitStrategy, numberOfRequiredPointsPerTree: Int, enableGreedy: Boolean, pq: BucketSearchPriorityQueue, depth: Int, prevScoresSum: Double, rtn: RandomTreeNode, query: Array[Double], state: SearchBucketsResult): Unit = {
    datasetSplitStrategy.processNonChildNodeDuringBucketSearch(numberOfRequiredPointsPerTree, enableGreedy, pq, depth, prevScoresSum, rtn, query, state)
  }

  def fillBucketIndexes(datasetSplitStrategy: DatasetSplitStrategy, requiredDataPoints: Int, enableGreedy: Boolean, root: RandomTree, query: Array[Double], state: SearchBucketsResult): Unit ={
    val pq = new OptimizedPriorityQueueSimpleImpl()
    var totalDataPoints = 0
    pq.add(0.0, 0, root)
    val reusedPQEntry = PQEntry(0.0, 0, null)
    while(totalDataPoints < requiredDataPoints && pq.remove(reusedPQEntry)){
      PerformanceCounters.exploreNode()
      val node = reusedPQEntry.node
      node match {
        case leaf: RandomTreeLeaf => {
          PerformanceCounters.processedLeaf(leaf.count, reusedPQEntry.depth)
          PerformanceCounters.expectedPointsInBuckets(leaf.count)
          totalDataPoints += leaf.count
          state.bucketIndexBuffer += leaf.leafId
          state.bucketScoreBuffer += reusedPQEntry.adjScore
        }
        case rn: RandomTreeNode => {
          processNonChildNode(datasetSplitStrategy, requiredDataPoints, enableGreedy, pq, reusedPQEntry.depth, reusedPQEntry.sumScores, rn, query, state)
        }
        case EmptyLeaf => {
        }
      }
    }

    //println("pos/all/examined " + pos + " " + all + " " + 100.0*pos/all + " " + totalNodes)
    //println("Total nodes: " + totalNodes + " in queue " + pq.size())
  }

  def getBucketIndexes(randomTrees: RandomTrees, inputQuery: Array[Double], scratchBuffer: NearestNeigbhorQueryScratchBuffer): SearchBucketsResult = {
    val numberOfRequiredPointsPerTree = settings.numberOfRequiredPointsPerTree
    val query = randomTrees.dimReductionTransform.transformQuery(inputQuery)
    val enableGreedy = settings.enableGreedy
    val datasetSplitStrategy = this.datasetSplitStrategy
    PerformanceCounters.numberOfRequiredPointsPerTree(numberOfRequiredPointsPerTree)
    val state = new SearchBucketsResult(query.length, scratchBuffer)
    val trees = randomTrees.trees
    var i = 0
    while(i < trees.length){
      val tree = trees(i)
      val (childTree, modifiedQuery) = tree match {
        case root: RandomTreeNodeRoot => {
          (root.child, root.modifyQuery(query))
        }
        case _ => (tree, query)
      }
      fillBucketIndexes(datasetSplitStrategy, numberOfRequiredPointsPerTree, enableGreedy, childTree, modifiedQuery, state)
      i += 1
    }
    state
  }

}
