package com.stefansavev.randomprojections.implementation

import com.stefansavev.randomprojections.buffers.IntArrayBuffer
import com.stefansavev.randomprojections.datarepr.dense.PointIndexes
import com.stefansavev.randomprojections.interface.{BucketCollector, Index}

class BucketCollectorImpl(totalRows: Int) extends BucketCollector{
  var leafId = 0
  var starts = new IntArrayBuffer()
  var pointIds = new IntArrayBuffer()
  var globalIndex = 0

  def collectPoints(names: PointIndexes): RandomTreeLeaf = {
    val leaf = RandomTreeLeaf(leafId, names.size)

    starts += globalIndex
    globalIndex += names.size
    pointIds ++= names.indexes

    leafId += 1

    leaf
  }

  def getIntArrayAndClear(buffer: IntArrayBuffer): Array[Int] = {
    val result = buffer.toArray
    buffer.clear()
    result
  }

  def build(pointSignatures: PointSignatures, labels: Array[Int]): Index = {
    starts += globalIndex
    val leaf2Points = new Leaf2Points(getIntArrayAndClear(starts), getIntArrayAndClear(pointIds))
    val index = new IndexImpl(pointSignatures, labels.length, leaf2Points, labels)
    index
  }

}