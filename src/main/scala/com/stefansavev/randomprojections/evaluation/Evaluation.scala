package com.stefansavev.randomprojections.evaluation

import java.io.PrintWriter

import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.implementation.{KNN, KNNS}

import scala.collection.mutable.ArrayBuffer

object Evaluation {

  def rescore(dataFrame: DataFrameView, knns: Array[KNNS], threshold: Double, kNeigbhor: Int): Array[KNNS] = {
    val output = ArrayBuffer[KNNS]()
    for(knn <- knns){
      val other_knns = knn.neighbors.filter(nn => nn.neighborId != knn.pointId && nn.count >= threshold).take(kNeigbhor)
      val buff = ArrayBuffer[(Double, KNN)]()
      for(nn <- other_knns) {
        val dist = dataFrame.dist(knn.pointId, nn.neighborId) // diff.values.map(v=>v*v).sum
        val newNN = new KNN(nn.neighborId, nn.count, nn.label, dist)
        buff += ((dist, newNN))
      }
      val newNNs = buff.toArray.sortBy({case (dist,nn)=> dist}).map(_._2)
      output += (KNNS(knn.k, knn.pointId, newNNs))
    }
    output.toArray
  }

  def evaluate(labels: Array[Int], knns: Array[KNNS], threshold: Int, kNeigbhor: Int): Double = {
    var missing = 0
    var matching = 0.0
    var present = 0.0
    var numNeighborsTotal = 0.0
    for((lab_i, knn) <- labels.zip(knns)){
      val other_knns = knn.neighbors.filter(nn => nn.neighborId != knn.pointId && nn.count >= threshold)
      numNeighborsTotal += other_knns.length
      if (other_knns.length < kNeigbhor){
        missing += 1
      }
      else{
        val neighbor_id = other_knns(kNeigbhor - 1).neighborId
        val lab_j = other_knns(kNeigbhor - 1).label
        matching += (if (lab_i == lab_j) 1.0 else 0.0)
        present += 1.0
      }
    }
    val accuracy = (100.0*matching/present)
    println("------------------")
    println("kNeigbhor: " + kNeigbhor + "   missing: " + missing)
    println("kNeigbhor: " + kNeigbhor + "   per matching: " + accuracy)
    println("avg. neighbors per query: " + numNeighborsTotal/labels.length)
    accuracy
  }

  def dumpLabels(fileName: String, labels: Array[String], knns: Array[KNNS], threshold: Int, kNeigbhor: Int): Unit = {
    val writer = new PrintWriter(fileName)
    var missing = 0
    var present = 0.0
    for((lab_i, knn) <- labels.zip(knns)){
      val other_knns = knn.neighbors.filter(nn => nn.neighborId != knn.pointId && nn.count >= threshold).take(kNeigbhor)
      if (other_knns.length < kNeigbhor){
        missing += 1
      }

      {
        writer.write("******" + lab_i + "\n")
        for(k <- 0 until other_knns.length) {
          val lab_j: String = labels(other_knns(k).neighborId)
          writer.write(lab_i + "\t" + lab_j + "\t" + other_knns(k).count + "\t" + other_knns(k).dist + "\n")
        }
        present += 1.0
      }
    }

    writer.close()
    println("------------------")
    println("kNeigbhor: " + kNeigbhor + "   missing: " + missing)
  }

  def evaluateWeightedNN(labels: Array[Int], knns: Array[KNNS], threshold: Int, kNeigbhor: Int): Unit = {
    var missing = 0
    var matching = 0.0
    var present = 0.0
    for((lab_i, knn) <- labels.zip(knns)){
      val other_knns = knn.neighbors.filter(nn => nn.neighborId != knn.pointId && nn.count >= threshold)
      if (other_knns.length < 2){
        missing += 1
      }
      else{
        //val neighbor_id = other_knns(kNeigbhor - 1).neighborId
        val lab_j = other_knns.take(kNeigbhor).map(_.label).groupBy(k => k).map({case (k,v) => (k, v.sum)}).toArray.sortBy(-_._2).toList.head._1
        matching += (if (lab_i == lab_j) 1.0 else 0.0)
        present += 1.0
      }
    }
    println("------------------")
    println("kNeigbhor: " + kNeigbhor + "   missing: " + missing)
    println("kNeigbhor: " + kNeigbhor + "   per matching: " + (100.0*matching/present))
  }

}
