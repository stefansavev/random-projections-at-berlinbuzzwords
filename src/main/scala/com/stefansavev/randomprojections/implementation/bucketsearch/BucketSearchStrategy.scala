package com.stefansavev.randomprojections.implementation.bucketsearch

import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer
import com.stefansavev.randomprojections.implementation.{RandomTrees, SearchBucketsResult}

trait BucketSearchSettings

abstract class BucketSearchStrategy {
  def getBucketIndexes(randomTrees: RandomTrees, query: Array[Double], scratch: NearestNeigbhorQueryScratchBuffer): SearchBucketsResult
}

