package com.stefansavev.randomprojections.implementation

import java.io._

import com.stefansavev.core.serialization.core.IntArraySerializer
import com.stefansavev.randomprojections.buffers.IntArrayBuffer
import com.stefansavev.randomprojections.datarepr.dense.PointIndexes
import com.stefansavev.randomprojections.datarepr.dense.store.FixedLengthBuffer
import com.stefansavev.randomprojections.interface.{BucketCollector, Index}
import com.typesafe.scalalogging.StrictLogging

object BucketCollectorImplUtils{
  val partitionFileSuffix = "_partition_"
  def fileName(dirName: String, partitionId: Int): String = {
    (new File(dirName, partitionFileSuffix + partitionId)).getAbsolutePath
  }
}

class BucketCollectorImpl(backingDir: String, totalRows: Int) extends BucketCollector with StrictLogging{
  val pointIdsThreshold = 1 << 20

  var leafId = 0
  var starts = new IntArrayBuffer()
  var pointIds = new IntArrayBuffer()
  var globalIndex = 0
  var numPartitions = 0

  def savePartial(): Unit = {
    val fileName = BucketCollectorImplUtils.fileName(backingDir, numPartitions)
    val outputStream = new BufferedOutputStream(new FileOutputStream(fileName))
    logger.info(s"Writing partial buckets to file ${fileName}")
    IntArraySerializer.write(outputStream, starts.toArray())
    IntArraySerializer.write(outputStream, pointIds.toArray())
    outputStream.close()
    starts = new IntArrayBuffer()
    pointIds = new IntArrayBuffer()
    numPartitions += 1
  }

  def collectPoints(names: PointIndexes): RandomTreeLeaf = {
    val leaf = RandomTreeLeaf(leafId, names.size)

    starts += globalIndex
    globalIndex += names.size
    pointIds ++= names.indexes
    leafId += 1

    if (pointIds.size > pointIdsThreshold){
      savePartial()
    }
    leaf
  }

  def getIntArrayAndClear(buffer: IntArrayBuffer): Array[Int] = {
    val result = buffer.toArray
    buffer.clear()
    result
  }

  def build(pointSignatures: PointSignatures, labels: Array[Int]): Index = {
    starts += globalIndex
    savePartial()
    //val leaf2Points = new Leaf2Points(getIntArrayAndClear(starts), getIntArrayAndClear(pointIds))
    val index = new IndexImpl(pointSignatures, labels.length, Some((backingDir, leafId + 1, globalIndex, numPartitions)), null, labels)
    index
  }
}

object BucketCollectorImpl{
  def mergeLeafData(backingDir: String, startBufferLen: Int, numPoints: Int, numPartitions: Int): Leaf2Points = {
    val starts = new FixedLengthBuffer[Int](startBufferLen)
    val pointsIds = new FixedLengthBuffer[Int](numPoints)
    for(i <- 0 until numPartitions){
      val fileName = BucketCollectorImplUtils.fileName(backingDir, i)
      val inputStream = new BufferedInputStream(new FileInputStream(fileName))
      starts ++= IntArraySerializer.read(inputStream)
      pointsIds ++= IntArraySerializer.read(inputStream)
      inputStream.close()
    }
    new Leaf2Points(starts.array, pointsIds.array)
  }
}