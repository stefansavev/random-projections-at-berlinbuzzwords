package com.stefansavev.randomprojections.utils

import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.implementation.{KNNS, Searcher}

import scala.collection.mutable.ArrayBuffer

object AllNearestNeighborsForDataset {

  def getTopNearestNeighborsForAllPointsTestDataset(topNearestNeighbors: Int, searcher: Searcher, testDataset: DataFrameView): Array[KNNS] = {
    val knnsBuffer = ArrayBuffer[KNNS]()
    val numRows = testDataset.numRows

    var i = 0
    while(i < numRows){
      val query = testDataset.getPointAsDenseVector(i)
      val knns = searcher.getNearestNeighborsByVector(query, topNearestNeighbors)
      knnsBuffer += (knns.copy(pointId = i))
      i += 1
      if (i % 1000 == 0){
        println(i)
      }
    }
    knnsBuffer.toArray
  }

  def getTopNearestNeighborsForAllPoints(topNearestNeighbors: Int, searcher: Searcher): Array[KNNS] = {
    val trainingSet = searcher.getSettings().trainingSet
    getTopNearestNeighborsForAllPointsTestDataset(topNearestNeighbors, searcher, trainingSet)
  }
}
