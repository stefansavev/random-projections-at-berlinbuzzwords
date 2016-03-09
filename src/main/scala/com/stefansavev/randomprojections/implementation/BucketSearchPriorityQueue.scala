package com.stefansavev.randomprojections.implementation

case class PQEntry(var sumScores: Double, var depth: Int, var node: RandomTree) extends Comparable[PQEntry] {
  def adjustScore(depth: Int, score: Double): Double = if (depth == 0.0) 0.0 /*to make sure progress id made*/ else sumScores / Math.sqrt(depth)

  //(sumScores*sumScores) /depth
  var adjScore = adjustScore(depth, sumScores)

  override def compareTo(o: PQEntry): Int = {
    o.adjScore.compareTo(this.adjScore)
  }

  def copyFrom(other: PQEntry): Unit = {
    sumScores = other.sumScores
    depth = other.depth
    node = other.node
    adjScore = adjustScore(depth, sumScores)
  }
}

abstract class BucketSearchPriorityQueue {
  def remove(pqEntryOutput: PQEntry): Boolean

  def add(sumScores: Double, depth: Int, node: RandomTree): Unit
}

class OptimizedPriorityQueueSimpleImpl extends BucketSearchPriorityQueue {

  import java.util.PriorityQueue

  val pq = new PriorityQueue[PQEntry]()

  override def remove(pqEntryOutput: PQEntry): Boolean = {
    if (pq.size() > 0) {
      val entry = pq.remove()
      pqEntryOutput.copyFrom(entry)
      true
    }
    else {
      false
    }
  }

  override def add(sumScores: Double, depth: Int, node: RandomTree): Unit = {
    val entry = PQEntry(sumScores, depth, node)
    pq.add(entry)
  }
}