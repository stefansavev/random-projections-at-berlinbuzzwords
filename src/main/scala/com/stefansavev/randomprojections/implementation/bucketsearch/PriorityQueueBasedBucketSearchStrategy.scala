package com.stefansavev.randomprojections.implementation.bucketsearch

import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.tuning.PerformanceCounters

case class PriorityQueueBasedBucketSearchSettings(numberOfRequiredPointsPerTree: Int, enableGreedy: Boolean = false) extends BucketSearchSettings

class PriorityQueueBasedBucketSearchStrategy(settings: PriorityQueueBasedBucketSearchSettings) extends BucketSearchStrategy {

  def processNonChildNode(numberOfRequiredPointsPerTree: Int, enableGreedy: Boolean, pq: BucketSearchPriorityQueue, depth: Int, prevScoresSum: Double, rtn: RandomTreeNode, query: Array[Double], state: SearchBucketsResult): Unit = {
    val projVector = rtn.projVector
    val sparseVec = projVector.asInstanceOf[HadamardProjectionVector].signs
    val dim = sparseVec.ids.length
    val input = state.tmpInput

    var i = 0
    var sum = 0.0
    while(i < dim){
      val columnId = sparseVec.ids(i)
      val p = sparseVec.values(i)*query(columnId) //- means(i)
      input(i) = p
      sum += p*p
      i += 1
    }

    sum = Math.sqrt(sum + HadamardUtils.eps)
    //TODO: This normalization is not necessary
    i = 0
    while(i < dim){
      input(i) /= sum
      i += 1
    }

    val output = state.tmpOutput
    HadamardUtils.multiplyInto(dim, input, output)
    //greedy

    var requireFullProcessing = true
    if (enableGreedy) {
      val children = rtn.children
      val bestBucketIndex = HadamardUtils.constrainedArgAbsValueMax(dim, output, children)
      val absScore = HadamardUtils.getAbsValue(dim, output, bestBucketIndex)
      val bestChild = children(bestBucketIndex)
      if (bestChild != null && bestChild.getCount >= numberOfRequiredPointsPerTree) {
        val sumScores = (prevScoresSum + absScore)
        pq.add(sumScores, depth + 1, bestChild)
        PerformanceCounters.processNonLeaf()
        requireFullProcessing = false
      }
    }

    if (requireFullProcessing) {
      i = 0
      while (i < dim) {
        val value = output(i)
        val bucketIndex = if (value > 0.0) i else if (value < 0.0) (i + dim) else 2 * dim
        val absScore = Math.abs(value)
        val child = rtn.children(bucketIndex)
        if (child != null) {
          val sumScores = (prevScoresSum + absScore)
          pq.add(sumScores, depth + 1, child)
          PerformanceCounters.processNonLeaf()
        }
        i += 1
      }
    }
  }

  def fillBucketIndexes(requiredDataPoints: Int, enableGreedy: Boolean, root: RandomTree, query: Array[Double], state: SearchBucketsResult): Unit ={
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
          processNonChildNode(requiredDataPoints, enableGreedy, pq, reusedPQEntry.depth, reusedPQEntry.sumScores, rn, query, state)
        }
        case EmptyLeaf => {
        }
      }
    }

    //println("pos/all/examined " + pos + " " + all + " " + 100.0*pos/all + " " + totalNodes)
    //println("Total nodes: " + totalNodes + " in queue " + pq.size())
  }

  def getBucketIndexes(randomTrees: RandomTrees, query: Array[Double], scratchBuffer: NearestNeigbhorQueryScratchBuffer): SearchBucketsResult = {
    val numberOfRequiredPointsPerTree = settings.numberOfRequiredPointsPerTree
    val enableGreedy = settings.enableGreedy
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
      fillBucketIndexes(numberOfRequiredPointsPerTree, enableGreedy, childTree, modifiedQuery, state)
      i += 1
    }
    state
  }

}
