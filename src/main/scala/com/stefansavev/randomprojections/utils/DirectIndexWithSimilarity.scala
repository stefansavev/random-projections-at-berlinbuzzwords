package com.stefansavev.randomprojections.utils

import com.stefansavev.randomprojections.buffers.{DoubleArrayBuffer, IntArrayBuffer}

class DirectIndexWithSimilarity(offsets: Array[Int], contextIds: Array[Int], similarities: Array[Double]){
  def numDataPoints(): Int = offsets.length - 1

  def getContextIdsAndSimilarities(queryId: Int): (Array[Int], Array[Double]) = {
    val start = offsets(queryId)
    val end = offsets(queryId + 1)
    val len = end - start
    val outputIds = Array.ofDim[Int](len)
    val outputSimilarities = Array.ofDim[Double](len)
    System.arraycopy(contextIds, start, outputIds, 0, len)
    System.arraycopy(similarities, start, outputSimilarities, 0, len)
    (outputIds, outputSimilarities)
  }
}

class DirectIndexWithSimilarityBuilder {
  var offsets = {val buffer = new IntArrayBuffer(1024); buffer += 0; buffer}

  var contextIds = new IntArrayBuffer(1024)
  var scores = new DoubleArrayBuffer(1024)

  var nextPointId = 0

  def addContexts(pointId: Int, pointContextIds: Array[Int], similarities: Array[Double]): Unit = {
    if (pointId != nextPointId){
      throw new IllegalStateException("Invalid queryId. Query ids should be added consequtively")
    }

    contextIds ++= pointContextIds
    scores ++= similarities

    val prevOffset = offsets(pointId)
    val thisOffset = prevOffset + pointContextIds.length
    offsets += thisOffset
    nextPointId += 1
  }

  def build(): DirectIndexWithSimilarity = {
    val offsetsArray = offsets.toArray()
    val scoresArray = scores.toArray()
    this.offsets = null //release immediately
    val contextIdsAsArray = contextIds.toArray()
    this.contextIds = null
    this.scores = null
    new DirectIndexWithSimilarity(offsetsArray, contextIdsAsArray, scoresArray)
  }
}
