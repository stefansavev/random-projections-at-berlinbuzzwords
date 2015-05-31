package com.stefansavev.randomprojections.interface

import com.stefansavev.randomprojections.datarepr.dense.PointIndexes
import com.stefansavev.randomprojections.implementation.RandomTreeLeaf
import com.stefansavev.randomprojections.utils.Utils

//collects the points into buckets
trait BucketCollector{
  def collectPoints(pointIds: PointIndexes): RandomTreeLeaf //returns leaf id
  def build(labels: Array[Int]): Index
}
