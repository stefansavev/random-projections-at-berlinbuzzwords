package com.stefansavev.randomprojections.interface

import com.stefansavev.randomprojections.datarepr.dense.PointIndexes
import com.stefansavev.randomprojections.implementation.{PointSignatures, RandomTreeLeaf}

//collects the points into buckets
trait BucketCollector{
  def collectPoints(pointIds: PointIndexes): RandomTreeLeaf //returns leaf id
  def build(pointSignatures: PointSignatures, labels: Array[Int]): Index
}
