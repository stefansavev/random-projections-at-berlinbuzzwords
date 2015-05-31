package com.stefansavev.randomprojections.implementation.bucketsearch

import com.stefansavev.randomprojections.implementation.{SearchBucketsResult, RandomTrees}
import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer

trait BucketSearchSettings

abstract class BucketSearchStrategy {
  def getBucketIndexes(randomTrees: RandomTrees, query: Array[Double], scratch: NearestNeigbhorQueryScratchBuffer): SearchBucketsResult
}

