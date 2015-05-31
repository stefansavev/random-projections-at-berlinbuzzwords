package com.stefansavev.randomprojections.utils

import com.stefansavev.randomprojections.implementation.{KNNS, Searcher}

import scala.collection.mutable.ArrayBuffer

object AllNearestNeighborsForDataset {

  def getTopNearestNeighborsForAllPoints(topNearestNeighbors: Int, searcher: Searcher): Array[KNNS] = {
    val knnsBuffer = ArrayBuffer[KNNS]()
    val numRows = searcher.getSettings().trainingSet.numRows

    var i = 0
    while(i < numRows){
      val knns = searcher.getNearestNeighborsById(i, topNearestNeighbors)
      knnsBuffer += knns
      i += 1
      if (i % 1000 == 0){
        println(i)
      }
    }
    knnsBuffer.toArray
  }


}
