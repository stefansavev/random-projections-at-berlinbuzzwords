package com.stefansavev.randomprojections.implementation.bucketsearch

import com.stefansavev.randomprojections.datarepr.sparse.BinarySparseVector8
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer
import com.stefansavev.randomprojections.tuning.PerformanceCounters

case class SimpleEntry(var sumScores: Double, var depth: Int, var nodePointer: Int) extends Comparable[SimpleEntry] {
  def adjustScore(depth: Int, score: Double): Double = {
    if (depth == 0.0)
      0.0 /*to make sure progress is made*/
    else
      sumScores / Math.sqrt(depth)
  }

  var adjScore = adjustScore(depth, sumScores)

  override def compareTo(o: SimpleEntry): Int = {
    o.adjScore.compareTo(this.adjScore)
  }

  def copyFrom(other: SimpleEntry): Unit = {
    sumScores = other.sumScores
    depth = other.depth
    nodePointer = other.nodePointer
    adjScore = adjustScore(depth, sumScores)
  }
}

class PriorityQueueSimpleEntry {

  import java.util.PriorityQueue

  val pq = new PriorityQueue[SimpleEntry]()

  def remove(pqEntryOutput: SimpleEntry): Boolean = {
    if (pq.size() > 0) {
      val entry = pq.remove()
      pqEntryOutput.copyFrom(entry)
      true
    }
    else {
      false
    }
  }

  def add(sumScores: Double, depth: Int, nodePointer: Int): Unit = {
    val entry = SimpleEntry(sumScores, depth, nodePointer)
    pq.add(entry)
  }
}

class EfficientlyStoredTreeBucketSearchStrategy(datasetSplitStrategy: DatasetSplitStrategy, settings: PriorityQueueBasedBucketSearchSettings) extends BucketSearchStrategy {

  def processNonChildNodeDuringBucketSearchOpt(reader: TreeReader, nonLeaf: MutableTreeNonLeaf, numberOfRequiredPointsPerTree: Int, pq: PriorityQueueSimpleEntry, depth: Int, prevScoresSum: Double, nodePointer: Int, query: Array[Double], state: SearchBucketsResult): Unit = {
    reader.decodeNonLeaf(nodePointer, nonLeaf)

    val sparseVec8 = new BinarySparseVector8(nonLeaf.proj) //TODO: remove // rtn_i.getProjVector()

    val dim = 8
    val input = state.tmpInput

    var sum = sparseVec8.multiplyVectorComponentWiseReturnSumOfSquares(query, input)
    var i = 0
    sum = Math.sqrt(sum + HadamardUtils.eps)

    i = 0
    while (i < dim) {
      input(i) /= sum
      i += 1
    }

    val output = state.tmpOutput
    OptimizedHadamard.multiplyInto(dim, input, output)

    i = 0

    val children = nonLeaf.childArray
    while (i < dim) {
      val value = output(i)
      val bucketIndex = if (value > 0.0) i else if (value < 0.0) (i + dim) else 2 * dim
      val absScore = Math.abs(value)
      val child = children(bucketIndex)
      if (child >= 0) {
        val sumScores = (prevScoresSum + absScore)
        pq.add(sumScores, depth + 1, child)
        PerformanceCounters.processNonLeaf()
      }
      //otherwise there is no data
      i += 1
    }
  }

  def fillBucketIndexes(requiredDataPoints: Int, query: Array[Double], rootNode: EfficientlyStoredTree, state: SearchBucketsResult): Unit = {
    val numberOfRequiredPointsPerTree = settings.numberOfRequiredPointsPerTree
    var totalDataPoints = 0
    val treeReader = rootNode.treeReader

    val reusedLeaf = treeReader.buildMutableTreeLeaf()
    val reusedNonLeaf = treeReader.buildMutableNonLeaf()

    val pq = new PriorityQueueSimpleEntry()
    pq.add(0.0, 0, 0)
    val reusedPQEntry = new SimpleEntry(0.0, 0, -1)

    while (totalDataPoints < requiredDataPoints && pq.remove(reusedPQEntry)) {
      PerformanceCounters.exploreNode()
      val nodePointer = reusedPQEntry.nodePointer

      if (treeReader.isLeaf(nodePointer)) {
        //handle the leaf
        treeReader.decodeLeaf(nodePointer, reusedLeaf)
        PerformanceCounters.processedLeaf(reusedLeaf.count, 0)
        PerformanceCounters.expectedPointsInBuckets(reusedLeaf.count)
        totalDataPoints += reusedLeaf.count

        state.bucketIndexBuffer += reusedLeaf.leafId
        state.bucketScoreBuffer += 0.0 //not used
      }
      else {
        val depth = reusedPQEntry.depth
        processNonChildNodeDuringBucketSearchOpt(
          treeReader,
          reusedNonLeaf,
          numberOfRequiredPointsPerTree,
          pq,
          depth,
          reusedPQEntry.sumScores,
          nodePointer,
          query,
          state)
      }
    }
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
    while (i < trees.length) {
      val tree = trees(i)

      val (childTree, modifiedQuery) = tree match {
        case root: RandomTreeNodeRoot => {
          val rotatedQuery = root.modifyQuery(query)
          (root.child, rotatedQuery)
        }
        case _ => (tree, query)
      }
      val subTree = childTree.asInstanceOf[EfficientlyStoredTree]
      fillBucketIndexes(numberOfRequiredPointsPerTree, modifiedQuery, subTree, state)
      i += 1
    }
    state
  }

}
