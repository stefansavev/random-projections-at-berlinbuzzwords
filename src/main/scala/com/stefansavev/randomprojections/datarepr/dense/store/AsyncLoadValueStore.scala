package com.stefansavev.randomprojections.datarepr.dense.store

import java.io.{FileInputStream, BufferedInputStream, File}

import com.stefansavev.randomprojections.actors.Application
import com.stefansavev.randomprojections.buffers.LongArrayBuffer
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.serialization.ValueStoreSerializationExt
import com.stefansavev.randomprojections.utils.Utils

object AsyncLoadValueStore {
  val partitionFileNamePrefix = "_dataset_partition_"

  def getFileName(dirName: String): String = {
    val newFile = new File(dirName, partitionFileNamePrefix + "_async")
    newFile.getAbsolutePath
  }

  type TupleType = (String, Array[Long], Int, Int, Int, Int)

  def fromTuple(t: TupleType): AsyncLoadValueStore = {
    val (backingDir, chunkOffsets, numPartitions, numPointsInPartition, numTotalRows, numColumns) = t
    new AsyncLoadValueStore(backingDir, chunkOffsets, numPartitions, numPointsInPartition, numTotalRows, numColumns)
  }
}

class AsyncLoadValueStore(backingDir: String, chunkOffsets: Array[Long], numPartitions: Int, numPointsInPartition: Int, numTotalRows: Int, numColumns: Int) extends ValuesStore {

  def toTuple(): AsyncLoadValueStore.TupleType = (backingDir, chunkOffsets, numPartitions, numPointsInPartition, numTotalRows, numColumns)

  var underlyingStore: ValuesStore = null
  var currentPartition = -1

  def getBuilderType: StoreBuilderType = {
    if (underlyingStore == null) {
      transformRowId(0) //call for sideeffect to load the data
    }
    underlyingStore.getBuilderType
  }

  def getPartition(rowId: Int): Int = {
    rowId / numPointsInPartition //TODO: replace by bit shift
  }

  def loadStoreFromPartition(inputStream: BufferedInputStream, partitionId: Int): ValuesStore = {
    import ValueStoreSerializationExt._
    val offset = chunkOffsets(partitionId)
    inputStream.skip(offset)
    val size = (chunkOffsets(partitionId + 1) - offset).toInt
    val input: Array[Byte] = new Array[Byte](size)
    inputStream.read(input)
    val store = ValuesStore.fromBytes(input)
    store
  }

  def loadStoreFromPartition(partitionId: Int): ValuesStore = {
    val fileName = AsyncLoadValueStore.getFileName(backingDir)
    val inputStream = new BufferedInputStream(new FileInputStream(fileName))
    val store = loadStoreFromPartition(inputStream, partitionId)
    inputStream.close()
    store
  }


  def transformRowId(rowId: Int): Int = {

    val partitionId = getPartition(rowId)
    // println("rowid - partitionId " + rowId + " " + partitionId)
    if (partitionId != currentPartition) {
      underlyingStore = loadStoreFromPartition(partitionId)

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
    val head: ValuesStore = loadStoreFromPartition(0)
    val rest = Iterator.range(1, numPartitions).map(partitionId => loadStoreFromPartition(partitionId))
    val iter = Iterator(head) ++ rest
    val mergedStores = head.getBuilderType.getBuilder(numColumns).merge(numTotalRows, iter)
    mergedStores
  }
}

class AsyncStoreBuilder(backingDir: String, numCols: Int, underlyingBuilder: StoreBuilderType) extends ValuesStoreBuilder {
  var currentRow = 0
  val numPointsInPartition = 1 << 14
  var currentPartition = 0
  var currentBuilder = underlyingBuilder.getBuilder(numCols)
  val writePositions = new LongArrayBuffer()
  val outputFileName = AsyncLoadValueStore.getFileName(backingDir)
  val writer = Application.createAsyncFileWriter(outputFileName, "AsyncStoreBuilder")
  val supervisor = Application.createWriterSupervisor("SupervisorAsyncStoreBuilder", 10, Array(writer))
  var currentPosition = 0

  def getCurrentRowIndex: Int = {
    currentRow
  }

  def savePartition(): Unit = {
    import ValueStoreSerializationExt._
    println("saving partition " + currentPartition)
    val currentStore = currentBuilder.build() //serialize to bytes and send them to the async serializer
    val bytes = currentStore.toBytes()
    val writerId = 0
    supervisor.write(writerId, bytes, currentPosition)
    writePositions += currentPosition
    currentPosition += bytes.length
    currentBuilder = underlyingBuilder.getBuilder(numCols)
    currentPartition += 1
  }

  def addValues(values: Array[Double]): Unit = {
    if (currentBuilder.getCurrentRowIndex >= numPointsInPartition) {
      savePartition()
    }
    currentBuilder.addValues(values)
    currentRow += 1
  }

  def build(): ValuesStore = {
    if (currentBuilder.getCurrentRowIndex > 0) {
      savePartition()
      //finalize
    }
    supervisor.waitUntilDone()
    writePositions += currentPosition
    new AsyncLoadValueStore(backingDir, writePositions.toArray, currentPartition, numPointsInPartition: Int, currentRow, numCols)
  }

  def merge(numTotalRows: Int, valueStores: Iterator[ValuesStore]): ValuesStore = {
    null //underlyingBuilder.getBuilder(numCols).merge(numTotalRows, valueStores)
  }

  def isFull: Boolean = Utils.internalError()

  def getContents(): Array[Byte] = Utils.internalError()
}