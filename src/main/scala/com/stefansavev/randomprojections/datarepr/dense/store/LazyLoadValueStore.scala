package com.stefansavev.randomprojections.datarepr.dense.store

import java.io.File

import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.implementation.HadamardUtils
import com.stefansavev.randomprojections.serialization.ValueStoreSerializationExt
import com.stefansavev.randomprojections.utils.Utils

object LazyLoadValueStore {
  val partitionFileNamePrefix = "_dataset_partition_"

  def getPartitionFileName(dirName: String, partitionId: Int): String = {
    val newFile = new File(dirName, partitionFileNamePrefix + partitionId)
    newFile.getAbsolutePath
  }

  type TupleType = (String, Int, Int, Int, Int)

  def fromTuple(t: TupleType): LazyLoadValueStore = {
    val (dirName, numPointsInPartition, numPartitions, numTotalRows, numColumns) = t
    new LazyLoadValueStore(dirName, numPointsInPartition, numPartitions, numTotalRows, numColumns)
  }
}

class LazyLoadValueStore(dirName: String, numPointsInPartition: Int, numPartitions: Int, numTotalRows: Int, numColumns: Int) extends ValuesStore {
  println("numPointsInPartition " + numPointsInPartition)
  if (!HadamardUtils.isPowerOf2(numPointsInPartition)) {
    Utils.internalError()
  }

  def toTuple(): LazyLoadValueStore.TupleType = (dirName, numPointsInPartition, numPartitions, numTotalRows, numColumns)

  var underlyingStore: ValuesStore = null
  var currentPartition = -1

  def getBuilderType: StoreBuilderType = {
    if (underlyingStore == null) {
      transformRowId(0) //call for side-effect to load the data
    }
    underlyingStore.getBuilderType
  }

  def getPartition(rowId: Int): Int = {
    rowId / numPointsInPartition //TODO: replace by bit shift
  }

  def transformRowId(rowId: Int): Int = {
    import ValueStoreSerializationExt._
    val partitionId = getPartition(rowId)
    if (partitionId != currentPartition) {
      underlyingStore = ValuesStore.fromFile(LazyLoadValueStore.getPartitionFileName(dirName, partitionId))
      currentPartition = partitionId
    }

    rowId % numPointsInPartition //TODO: replace by bit shift
  }

  def fillRow(rowId: Int, output: Array[Double], isPos: Boolean): Unit = {
    val tRowId = transformRowId(rowId)
    underlyingStore.fillRow(tRowId, output, isPos)
  }

  def setRow(rowId: Int, input: Array[Double]): Unit = {
    Utils.internalError() //this store is read only
  }

  def fillRow(rowId: Int, columnIds: Array[Int], output: Array[Double]): Unit = {
    val tRowId = transformRowId(rowId)
    underlyingStore.fillRow(tRowId, columnIds, output)
  }

  def multiplyRowComponentWiseBySparseVector(rowId: Int, sv: SparseVector, output: Array[Double]): Unit = {
    val tRowId = transformRowId(rowId)
    underlyingStore.multiplyRowComponentWiseBySparseVector(tRowId, sv, output)
  }

  def cosineForNormalizedData(query: Array[Double], rowId: Int): Double = {
    val tRowId = transformRowId(rowId)
    underlyingStore.cosineForNormalizedData(query, tRowId)
  }

  def loadAll(): ValuesStore = {
    import ValueStoreSerializationExt._
    def loadFile(partitionId: Int): ValuesStore = {
      ValuesStore.fromFile(LazyLoadValueStore.getPartitionFileName(dirName, partitionId))
    }
    val head: ValuesStore = loadFile(0)
    val rest = Iterator.range(1, numPartitions).map(partitionId => loadFile(partitionId))
    val iter = Iterator(head) ++ rest
    head.getBuilderType.getBuilder(numColumns).merge(numTotalRows, iter)
  }
}

class LazyLoadValuesStoreBuilder(backingDir: String, numCols: Int, underlyingBuilder: StoreBuilderType) extends ValuesStoreBuilder {
  var currentRow = 0
  val numPointsInPartition = 1 << 17
  var currentPartition = 0
  var currentBuilder = underlyingBuilder.getBuilder(numCols)

  def getCurrentRowIndex: Int = {
    currentRow
  }

  def savePartition(): Unit = {
    import ValueStoreSerializationExt._
    println("saving partition " + currentBuilder.getCurrentRowIndex)

    val currentStore = currentBuilder.build()
    val outputFile = LazyLoadValueStore.getPartitionFileName(backingDir, currentPartition)
    println("#output file: " + outputFile)
    if (new File(outputFile).exists()) {
      Utils.internalError()
    }
    currentStore.toFile(outputFile)
    currentBuilder = underlyingBuilder.getBuilder(numCols)
    currentPartition += 1
  }

  def addValues(values: Array[Double]): Unit = {
    if (currentBuilder.getCurrentRowIndex == numPointsInPartition) {
      savePartition()
    }
    currentBuilder.addValues(values)
    currentRow += 1
  }

  def build(): ValuesStore = {
    if (currentBuilder.getCurrentRowIndex > 0) {
      savePartition()
    }
    new LazyLoadValueStore(backingDir, numPointsInPartition, currentPartition, currentRow, numCols)
  }

  def merge(numTotalRows: Int, valueStores: Iterator[ValuesStore]): ValuesStore = {
    underlyingBuilder.getBuilder(numCols).merge(numTotalRows, valueStores)
  }

  def isFull: Boolean = Utils.internalError()

  def getContents(): Array[Byte] = Utils.internalError()
}