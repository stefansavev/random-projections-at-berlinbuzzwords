package com.stefansavev.randomprojections.datarepr.dense

import java.io.{FileInputStream, BufferedInputStream, File}

import akka.actor.Actor
import com.stefansavev.randomprojections.actors.Application
import com.stefansavev.randomprojections.buffers._
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.implementation.HadamardUtils
import com.stefansavev.randomprojections.serialization.DoubleArraySerializer
import com.stefansavev.randomprojections.serialization.core.Core
import com.stefansavev.randomprojections.utils.Utils

import scala.reflect.ClassTag

trait ValuesStore{
  def fillRow(rowId: Int, output: Array[Double], isPos: Boolean): Unit
  def setRow(rowId: Int, input: Array[Double]): Unit
  def fillRow(rowId: Int, columnIds: Array[Int], output: Array[Double]): Unit

  def multiplyRowComponentWiseBySparseVector(rowId: Int, sv: SparseVector, output: Array[Double]): Unit
  def cosineForNormalizedData(query: Array[Double], id: Int): Double

  def getBuilderType: StoreBuilderType
}

object ValuesStore{

}

object LazyLoadValueStore{
  val partitionFileNamePrefix = "_dataset_partition_"
  def getPartitionFileName(dirName: String, partitionId: Int): String = {
    val newFile = new File(dirName, partitionFileNamePrefix  + partitionId)
    newFile.getAbsolutePath
  }

  type TupleType = (String, Int, Int, Int, Int)

  def fromTuple(t: TupleType): LazyLoadValueStore = {
    val (dirName, numPointsInPartition, numPartitions, numTotalRows, numColumns) = t
    new LazyLoadValueStore(dirName, numPointsInPartition, numPartitions, numTotalRows, numColumns)
  }
}

class LazyLoadValueStore(dirName: String, numPointsInPartition: Int, numPartitions: Int, numTotalRows: Int, numColumns: Int) extends ValuesStore{
  println("numPointsInPartition " + numPointsInPartition)
  if (!HadamardUtils.isPowerOf2(numPointsInPartition)){
    Utils.internalError()
  }

  def toTuple(): LazyLoadValueStore.TupleType = (dirName, numPointsInPartition, numPartitions, numTotalRows, numColumns)

  var underlyingStore: ValuesStore = null
  var currentPartition = -1

  def getBuilderType: StoreBuilderType = {
    if (underlyingStore == null){
      transformRowId(0) //call for sideeffect to load the data
    }
    underlyingStore.getBuilderType
  }

  def getPartition(rowId: Int): Int = {
    rowId / numPointsInPartition //TODO: replace by bit shift
  }

  def transformRowId(rowId: Int): Int = {
    import com.stefansavev.randomprojections.serialization.ValueStoreSerializationExt._
    val partitionId = getPartition(rowId)
   // println("rowid - partitionId " + rowId + " " + partitionId)
    if (partitionId != currentPartition){
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
    import com.stefansavev.randomprojections.serialization.ValueStoreSerializationExt._
    def loadFile(partitionId: Int): ValuesStore = {
      ValuesStore.fromFile(LazyLoadValueStore.getPartitionFileName(dirName, partitionId))
    }
    val head:ValuesStore = loadFile(0)
    val rest = Iterator.range(1, numPartitions).map(partitionId => loadFile(partitionId))
    val iter = Iterator(head) ++ rest
    head.getBuilderType.getBuilder(numColumns).merge(numTotalRows, iter)
  }
}


object AsyncLoadValueStore{
  val partitionFileNamePrefix = "_dataset_partition_"

  def getFileName(dirName: String): String = {
    val newFile = new File(dirName, partitionFileNamePrefix  + "_async")
    newFile.getAbsolutePath
  }

  type TupleType = (String, Array[Long], Int, Int, Int, Int)

  def fromTuple(t: TupleType): AsyncLoadValueStore = {
    val (backingDir, chunkOffsets, numPartitions, numPointsInPartition, numTotalRows, numColumns) = t
    new AsyncLoadValueStore(backingDir, chunkOffsets, numPartitions, numPointsInPartition, numTotalRows, numColumns)
  }
}

class AsyncLoadValueStore(backingDir: String, chunkOffsets: Array[Long], numPartitions: Int, numPointsInPartition: Int, numTotalRows: Int, numColumns: Int) extends ValuesStore{

  def toTuple(): AsyncLoadValueStore.TupleType = (backingDir, chunkOffsets, numPartitions, numPointsInPartition, numTotalRows, numColumns)

  var underlyingStore: ValuesStore = null
  var currentPartition = -1

  def getBuilderType: StoreBuilderType = {
    if (underlyingStore == null){
      transformRowId(0) //call for sideeffect to load the data
    }
    underlyingStore.getBuilderType
  }

  def getPartition(rowId: Int): Int = {
    rowId / numPointsInPartition //TODO: replace by bit shift
  }

  def loadStoreFromPartition(partitionId: Int): ValuesStore = {
    import com.stefansavev.randomprojections.serialization.ValueStoreSerializationExt._

    val fileName = AsyncLoadValueStore.getFileName(backingDir)
    val inputStream = new BufferedInputStream(new FileInputStream(fileName))
    val offset = chunkOffsets(partitionId)
    inputStream.skip(offset)
    val size = (chunkOffsets(partitionId + 1) - offset).toInt
    val input: Array[Byte] = new Array[Byte](size)
    inputStream.read(input)
    val store  = ValuesStore.fromBytes(input)
    inputStream.close()
    store
  }

  def transformRowId(rowId: Int): Int = {

    val partitionId = getPartition(rowId)
    // println("rowid - partitionId " + rowId + " " + partitionId)
    if (partitionId != currentPartition){
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
    import com.stefansavev.randomprojections.serialization.ValueStoreSerializationExt._
    val head:ValuesStore = loadStoreFromPartition(0)
    val rest = Iterator.range(1, numPartitions).map(partitionId => loadStoreFromPartition(partitionId))
    val iter = Iterator(head) ++ rest
    head.getBuilderType.getBuilder(numColumns).merge(numTotalRows, iter)
  }
}


trait ValuesStoreBuilder{
  def getCurrentRowIndex: Int

  def isFull: Boolean
  def getContents(): Array[Byte]

  def addValues(values: Array[Double]): Unit
  def build(): ValuesStore

  def merge(numTotalRows: Int, valueStores: Iterator[ValuesStore]): ValuesStore
}

object ValuesStoreAsDoubleSerializationTags{
  val valuesStoreAsDouble = 1
  val valuesStoreAsBytes = 2
  val valuesStoreAsSingleByte = 3
  val lazyLoadValuesStore = 4
  val asyncLoadValuesStore = 5
}

class LazyLoadValuesStoreBuilder(backingDir: String, numCols: Int, underlyingBuilder: StoreBuilderType) extends ValuesStoreBuilder{
  var currentRow = 0
  val numPointsInPartition = 1 << 17 //1 << 17 //= 131072
  var currentPartition = 0
  var currentBuilder = underlyingBuilder.getBuilder(numCols)

  def getCurrentRowIndex: Int = {
    currentRow
  }

  def savePartition(): Unit = {
    import com.stefansavev.randomprojections.serialization.ValueStoreSerializationExt._
    println("saving partition " + currentBuilder.getCurrentRowIndex)

    val currentStore = currentBuilder.build()
    val outputFile = LazyLoadValueStore.getPartitionFileName(backingDir, currentPartition)
    println("#output file: " + outputFile)
    if (new File(outputFile).exists()){
      Utils.internalError()
    }
    currentStore.toFile(outputFile)
    currentBuilder = underlyingBuilder.getBuilder(numCols)
    currentPartition += 1
  }

  def addValues(values: Array[Double]): Unit = {
    if (currentBuilder.getCurrentRowIndex == numPointsInPartition){
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


class AsyncStoreBuilder(backingDir: String, numCols: Int, underlyingBuilder: StoreBuilderType) extends ValuesStoreBuilder{
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
    import com.stefansavev.randomprojections.serialization.ValueStoreSerializationExt._
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
    if (currentBuilder.getCurrentRowIndex >= numPointsInPartition){
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

object ValuesStoreAsDouble{
  type TupleType = (Int, Array[Double])

  def fromTuple(t: TupleType): ValuesStoreAsDouble = {
    val (numCols, data) = t
    new ValuesStoreAsDouble(numCols, data)
  }
}

class ValuesStoreAsDouble(val _numCols: Int, val data: Array[Double]) extends ValuesStore{

  def toTuple(): ValuesStoreAsDouble.TupleType = {
    (_numCols, data)
  }

  def getBuilderType: StoreBuilderType = StoreBuilderAsDoubleType

  def fillRow(rowId: Int, output: Array[Double], isPos: Boolean): Unit = {
    var offset = rowId*_numCols
    var j = 0
    while(j < _numCols){
      if (isPos){
        output(j) += data(offset)
      } else {
        output(j) -= data(offset)
      }
      offset += 1
      j += 1
    }
  }

  def setRow(rowId: Int, input: Array[Double]): Unit = {
    var offset = rowId*_numCols
    var j = 0
    while(j < _numCols){
      data(offset) = input(j)
      offset += 1
      j += 1
    }
  }


  def fillRow(rowId: Int, columnIds: Array[Int], output: Array[Double]): Unit = {
    val offset = rowId*_numCols
    var j = 0
    while(j < columnIds.length){
      val columnId = columnIds(j)
      output(columnId) = data(offset + columnId)
      j += 1
    }
  }

  override def multiplyRowComponentWiseBySparseVector(rowId: Int, sv: SparseVector, output: Array[Double]): Unit = {
    val offset = rowId*_numCols
    val columnIds = sv.ids
    val values = sv.values
    val dim = columnIds.length
    var j = 0
    while(j < dim){
      val columnId = columnIds(j)
      val value = values(j)
      output(j) = data(offset + columnId)*value
      j += 1
    }
  }

  def cosineForNormalizedData(query: Array[Double], id: Int): Double = {
    var offset = id*_numCols
    var j = 0
    var sum = 0.0
    while(j < _numCols){
      val v1 = query(j)
      val v2 = data(offset)
      val d = v1 - v2
      sum += d*d
      offset += 1
      j += 1
    }
    1.0 - 0.5*sum
  }
}

class ValuesStoreBuilderAsDouble(numCols: Int) extends ValuesStoreBuilder{
  val numValuesInPage = 1024
  val valuesBuffer = new DoubleArrayBuffer()
  var currentRow = 0

  def addValues(values: Array[Double]): Unit = {
    if (values.length != numCols){
      Utils.internalError()
    }
    valuesBuffer ++= values
    currentRow += 1
  }

  def isFull: Boolean = (currentRow >= numValuesInPage)

  def getContents: Array[Byte] = {
    val output = Array.ofDim[Byte](8*currentRow)
    DoubleArraySerializer.writeToBufferWithoutArrayLength(valuesBuffer.toArray(), output)
    output
  }

  def getCurrentRowIndex = currentRow

  def build(): ValuesStore = {
    val values = valuesBuffer.toArray()
    new ValuesStoreAsDouble(numCols, values)
  }

  def merge(numTotalRows: Int, valueStores: Iterator[ValuesStore]): ValuesStore = {
    val buffer = new FixedLengthBuffer[Double](numTotalRows*numCols)
    for(store <- valueStores){
      val typedStore = store.asInstanceOf[ValuesStoreAsDouble]
      buffer ++= typedStore.data
      if (typedStore._numCols != numCols){
        Utils.internalError()
      }
    }
    new ValuesStoreAsDouble(numCols, buffer.array)
  }
}

class ValuesStoreBuilderAsFloat{
}

object ValuesStoreAsBytes{
  type TupleType = (Int, Array[Short])

  def fromTuple(t: TupleType): ValuesStoreAsBytes = {
    val (numCols, data) = t
    new ValuesStoreAsBytes(numCols, data)
  }
}

object FloatToByteEncoder{
  def encodeValue(value: Double): Short = {
    val intRepr: Int = java.lang.Float.floatToRawIntBits(value.toFloat)
    val s = (intRepr >>> 16).toShort
    s
  }

  def decodeValue(s: Short): Double = {
    val bits = (s & 0xFFFF) << 16
    java.lang.Float.intBitsToFloat(bits)
  }

}

class ValuesStoreAsBytes(val _numCols: Int, val _data: Array[Short]) extends ValuesStore{

  def getBuilderType: StoreBuilderType = StoreBuilderAsBytesType

  def getAtIndex(index: Int): Double = {
    FloatToByteEncoder.decodeValue(_data(index))
  }

  def toTuple(): ValuesStoreAsBytes.TupleType = {
    (_numCols, _data)
  }

  def fillRow(rowId: Int, output: Array[Double], isPos: Boolean): Unit = {
    var offset = rowId*_numCols
    var j = 0
    while(j < _numCols){
      if (isPos){
        output(j) += getAtIndex(offset)
      } else {
        output(j) -= getAtIndex(offset)
      }
      offset += 1
      j += 1
    }
  }

  def setRow(rowId: Int, input: Array[Double]): Unit = {
    var offset = rowId*_numCols
    var j = 0
    while(j < _numCols){
      _data(offset) = FloatToByteEncoder.encodeValue(input(j))
      offset += 1
      j += 1
    }
  }


  def fillRow(rowId: Int, columnIds: Array[Int], output: Array[Double]): Unit = {
    val offset = rowId*_numCols
    var j = 0
    while(j < columnIds.length){
      val columnId = columnIds(j)
      output(columnId) = getAtIndex(offset + columnId)
      j += 1
    }
  }

  override def multiplyRowComponentWiseBySparseVector(rowId: Int, sv: SparseVector, output: Array[Double]): Unit = {
    val offset = rowId*_numCols
    val columnIds = sv.ids
    val values = sv.values
    val dim = columnIds.length
    var j = 0
    while(j < dim){
      val columnId = columnIds(j)
      val value = values(j)
      output(j) = getAtIndex(offset + columnId)*value
      j += 1
    }
  }

  def cosineForNormalizedData(query: Array[Double], id: Int): Double = {
    var offset = id*_numCols
    var j = 0
    var sum = 0.0
    while(j < _numCols){
      val v1 = query(j)
      val v2 = getAtIndex(offset)
      val d = v1 - v2
      sum += d*d
      offset += 1
      j += 1
    }
    1.0 - 0.5*sum
  }
}

class ValuesStoreBuilderAsBytes(numCols: Int) extends ValuesStoreBuilder{
  val valuesBuffer = new ShortArrayBuffer()
  var currentRow = 0

  /*
  def encodeValue(value: Double): Short = {
    val intRepr: Int = java.lang.Float.floatToRawIntBits(value.toFloat)
    val s = (intRepr >>> 16).toShort
    s
  }

  def decodeValue(s: Short): Double = {
    val bits = (s & 0xFFFF) << 16
    java.lang.Float.intBitsToFloat(bits)
  }

  def decodeEncode(value: Double): Short = {
    val s = encodeValue(value)
    val decoded = decodeValue(s)
    val error = value - decoded
    //println("orig: " + value + " decoded:" + value + " error:" + error)
    s
  }
  */

  def addValues(values: Array[Double]): Unit = {
    if (values.length != numCols){
      Utils.internalError()
    }
    var i = 0
    while(i < values.length){
      valuesBuffer += FloatToByteEncoder.encodeValue(values(i))
      i += 1
    }
    currentRow += 1
  }

  def getCurrentRowIndex = currentRow

  def build(): ValuesStore = {
    val values = valuesBuffer.toArray()
    new ValuesStoreAsBytes(numCols, values)
  }

  def merge(numTotalRows: Int, valueStores: Iterator[ValuesStore]): ValuesStore = {
    var numColumns = -1
    val buffer = new ShortArrayBuffer(numTotalRows)
    for(store <- valueStores){
      val typedStore = store.asInstanceOf[ValuesStoreAsBytes]
      buffer ++= typedStore._data
      if (numColumns >= 0 && typedStore._numCols != numColumns){
        Utils.internalError()
      }
      numColumns = typedStore._numCols
    }
    if (buffer.array.length != buffer.size){
      Utils.internalError()
    }
    new ValuesStoreAsBytes(numColumns, buffer.array)
  }

  def isFull: Boolean = Utils.internalError()
  def getContents(): Array[Byte] = Utils.internalError()
}

object FloatToSingleByteEncoder{
  val almostZero = java.lang.Float.intBitsToFloat(1) //use this as a representation of zero: 00000000000000000000000000000001

  def getExponent(value:Double): Int = {
    val intRepr: Int = java.lang.Float.floatToRawIntBits(value.toFloat)
    val exp = (intRepr >>> (32 - 9)) & 0xFF //treat it as int from 0 to 255
    exp
  }

  def encodeValue(minValue: Float, maxValue: Float, value: Double): Byte = {
    val normalizedValue = (value - minValue)/(maxValue - minValue)  //from 0 to 1
    //if (normalizedValue < 0.0 || normalizedValue >= 1.0){
    //  println("normalizedValue: " + normalizedValue)
    //  Utils.internalError()
    //}
    //check if randomized rounding works better for similarity
    val rounded: Int = Math.min(255, (normalizedValue*255.0).round.toInt)
    if (rounded < 0 || rounded > 255){
      Utils.internalError()
    }
    val asByte = rounded.toByte
    val backToInt = asByte & 0xFF
    if (backToInt != rounded){
      Utils.internalError()
    }
    //println(value + " normalized: " + normalizedValue + " " + rounded + " " + asByte)
    asByte
  }

  def decodeValue(minValue: Float, maxValue: Float, input: Byte): Double = {
    val asInt = (input & 0xFF).toFloat
    val decoded: Float = (maxValue - minValue)*(asInt)/255.0f + minValue
    //println("decoded: " + decoded)
    decoded.toDouble
  }

  //need to take special care of zero
  def encodeValue_(minExp: Int, maxExp: Int, value: Double): Byte = {
    //println("input: " + value)
    //println("original exp: " + getExponent(value))
    val adjustedExp = getExponent(value) - minExp //normalized exp
    //println("exp: " + adjustedExp)
    val range = maxExp - minExp
    //println("range: " + range)
    val numBitsToUse = 32 - Integer.numberOfLeadingZeros(range)
    //println("#bits to use " + numBitsToUse)

    //val freeBits = 8 - numBitsToUse
    //println("normalized exp " + exp + " range: " + (maxExp - minExp) + " " + numBitsToUse)
    val intRepr: Int = java.lang.Float.floatToRawIntBits(value.toFloat)
    //println("intRepr " + allBits(intRepr))
    val signBit = (intRepr >>> 31)
    //println("signBit " + allBits(signBit))
    //println("diff exp: " + adjustedExp + " " + allBits(adjustedExp))
    val expRepr = ((adjustedExp) << 1)
    //println("exp " + allBits(expRepr))
    val availableBits = 8 - numBitsToUse - 1

    val rest = ((intRepr << 9) >>> (32 - availableBits)) << (1 + numBitsToUse)
    //println("rest " + allBits(rest))
    val output = rest | expRepr | signBit
    //println("output " + allBits(output))
    output.toByte
  }

  def decodeValue_(minExp: Int, maxExp: Int, input: Byte): Double = {
    //rest | exp bits | sign
    val intRepr = input & 0xFF
    //println("intRe  " + allBits(intRepr))
    val signBit = (intRepr & 0x1) << 31
    //println("signbit: " + allBits(signBit))
    val range = maxExp - minExp
    val numBitsToUse = 32 - Integer.numberOfLeadingZeros(range)
    val availableBits = 8 - numBitsToUse - 1

    val s = 1 << (numBitsToUse - 1)
    val mask = (s | (s - 1)) << 1
    //1 left (sign bit in compressed), 24 right , 1 left (again because of sign bit)
    //24 - 2
    val expRepr = ((intRepr & mask) >> 1)
    //println("mask: " + allBits(mask))
    //println("expBits: " + allBits(expRepr))

    val exp = expRepr + minExp
    //println("exp: " + exp)
    val shiftedExp = exp << 23
    //println("shifted exp: " + allBits(shiftedExp))
    val rest = (intRepr >>> (numBitsToUse + 1))  << (32 - 9 - availableBits)
    //println("rest " + allBits(rest))
    val rawBits = signBit | shiftedExp  | rest
    //println("rawbits out: " + allBits(rawBits))
    val finalResult = java.lang.Float.intBitsToFloat(rawBits)
    //println("finalRes: " + finalResult)
    //throw new IllegalStateException()
    finalResult
  }

  def encodeValues_(values: Array[Double], minExponentPerRecord: IntArrayBuffer, maxExponentPerRecord: IntArrayBuffer, valuesBuffer: ByteArrayBuffer): Unit = {
    //if we have zeros or values too close to zero this will increase the range of the exponent
    //need to check for the usual case (what is the range of the exponents)
    //but because of the normalization by the length, all numbers are smaller than 1

    var i = 0
    var minExp = 255
    var maxExp = 0
    while (i < values.length){
      val exp = getExponent(values(i))
      minExp = Math.min(exp, minExp)
      maxExp = Math.max(exp, maxExp)
      i += 1
    }

    minExponentPerRecord += minExp //.toByte
    maxExponentPerRecord += maxExp //.toByte
    i = 0
    while(i < values.length){
      valuesBuffer += encodeValue(minExp, maxExp, values(i))
      i += 1
    }
  }

  def encodeValues(values: Array[Double], minValues: FloatArrayBuffer, maxValues: FloatArrayBuffer, valuesBuffer: ByteArrayBuffer): Unit = {
    //if we have zeros or values too close to zero this will increase the range of the exponent
    //need to check for the usual case (what is the range of the exponents)
    //but because of the normalization by the length, all numbers are smaller than 1


    var minValue = values(0).toFloat
    var maxValue = minValue
    var i = 1
    while (i < values.length){
      val value = values(i).toFloat
      minValue = Math.min(minValue, value)
      maxValue = Math.max(maxValue, value)
      i += 1
    }

    minValues += minValue
    maxValues += maxValue
    i = 0
    while(i < values.length){
      valuesBuffer += encodeValue(minValue, maxValue, values(i))
      i += 1
    }
  }

  def encodeValues(offsetPerRecord: Int, minValues: Array[Float], maxValues: Array[Float], offsetValues: Int, valuesBuffer: Array[Byte], values: Array[Double]): Unit = {
    var minValue = values(0).toFloat
    var maxValue = minValue
    var i = 1
    while (i < values.length){
      val value = values(i).toFloat
      minValue = Math.min(minValue, value)
      maxValue = Math.max(maxValue, value)
      i += 1
    }
    minValues(offsetPerRecord) = minValue
    maxValues(offsetPerRecord) = maxValue
    i = 0
    var k = offsetValues
    while(i < values.length){
      valuesBuffer(k) = encodeValue(minValue, maxValue, values(i))
      i += 1
      k += 1
    }
  }

  def decodeValues(offsetPerRecord: Int, minValues: Array[Float], maxValues: Array[Float], offsetValues: Int, valuesBuffer: Array[Byte], output: Array[Double]): Unit = {
    val minValue = minValues(offsetPerRecord)
    val maxValue = maxValues(offsetPerRecord)
    var k = offsetValues
    var j = 0
    var i = 0
    while(i < output.length){
      val encoded = valuesBuffer(k)
      val decoded = decodeValue(minValue, maxValue, encoded)
      output(i) = decoded
      i += 1
      j += 1
      k += 1
    }
  }
}

class ValuesStoreBuilderAsSingleByte(numCols: Int) extends ValuesStoreBuilder{
  //val minExponentPerRecord = new IntArrayBuffer()
  //val maxExponentPerRecord = new IntArrayBuffer()

  val minValuePerRecord = new FloatArrayBuffer()
  val maxValuePerRecord = new FloatArrayBuffer()
  //val rangeSizeInBits = new ByteArrayBuffer()

  val valuesBuffer = new ByteArrayBuffer()
  var currentRow = 0

  def allBits(i: Int):String = {
    val p = i.toBinaryString
    val extra = (32 - p.size)
    Array.range(0, extra).map(_=> "0").mkString("") + p
  }

  def decodeEncodeTestExp(value: Double): Unit = {
    val exp = FloatToSingleByteEncoder.getExponent(value) - 127 //the exponent is biased

    if (value > 0) {
      val logValue = (Math.log(value)/Math.log(2.0)).floor
      //println("+exp: " + exp + " " + logValue + " diff: " + (exp - logValue))
    }
    else{
      val logValue = (Math.log(-value)/Math.log(2.0)).floor
      println("-exp: " + exp + " " + logValue + " diff: " + (exp - logValue))
    }


  }

  def decodeEncode(minExp: Int, maxExp: Int, value: Double): Byte = {
    val encoded = FloatToSingleByteEncoder.encodeValue(minExp, maxExp, value)
    val decoded = FloatToSingleByteEncoder.decodeValue(minExp, maxExp, encoded)
    val error = value - decoded
    //println("!!! orig: " + value + " decoded:" + value + " error:" + error)
    encoded
  }

  def addValues(values: Array[Double]): Unit = {
    if (values.length != numCols){
      Utils.internalError()
    }

    FloatToSingleByteEncoder.encodeValues(values, minValuePerRecord, maxValuePerRecord, valuesBuffer)
    /*
    val decoded = Array.ofDim[Double](values.length)
    FloatToSingleByteEncoder.decodeValues(currentRow, minValuePerRecord.array, maxValuePerRecord.array, currentRow*numCols, valuesBuffer.array, decoded)
    values.zip(decoded).foreach{case (o,d) => {
      println("o/d/e " + o + " " + d + " " + (o - d))
    }}
    */
    currentRow += 1
  }

  def getCurrentRowIndex = currentRow

  def build(): ValuesStore = {
    //val values = valuesBuffer.toArray()
    new ValuesStoreAsSingleByte(numCols, minValuePerRecord.toArray(), maxValuePerRecord.toArray(), valuesBuffer.toArray())
  }

  def merge(numTotalRows: Int, valueStores: Iterator[ValuesStore]): ValuesStore = {
    println("totalnumrows: " + numTotalRows)
    val minValuePerRecord = new FixedLengthBuffer[Float](numTotalRows)
    val maxValuePerRecord = new FixedLengthBuffer[Float](numTotalRows)
    val valuesBuffer = new FixedLengthBuffer[Byte](numTotalRows*numCols)
    for(store <- valueStores){
      val typedStore = store.asInstanceOf[ValuesStoreAsSingleByte]
      println("minValuesLen: " + typedStore.minValues.length)
      minValuePerRecord ++= typedStore.minValues
      maxValuePerRecord ++= typedStore.maxValues
      valuesBuffer ++= typedStore._data
      if (typedStore._numCols != this.numCols){
        Utils.internalError()
      }
    }
    new ValuesStoreAsSingleByte(this.numCols, minValuePerRecord.array, maxValuePerRecord.array, valuesBuffer.array)
  }

  def isFull: Boolean = Utils.internalError()
  def getContents(): Array[Byte] = Utils.internalError()
}

class FixedLengthBuffer[T : ClassTag](val size: Int){
  val buffer = Array.ofDim[T](size)
  var offset = 0
  def ++= (values: Array[T]): Unit = {
    System.arraycopy(values, 0, buffer, offset, values.length)
    offset += values.length
  }
  def array = buffer
}

object ValuesStoreAsSingleByte{
  type TupleType = (Int, Array[Float], Array[Float], Array[Byte])

  def fromTuple(t: TupleType): ValuesStoreAsSingleByte = {
    val (numCols, minValues, maxValues, data) = t
    new ValuesStoreAsSingleByte(numCols, minValues, maxValues, data)
  }
}

class ValuesStoreAsSingleByte(val _numCols: Int, val minValues: Array[Float], val maxValues: Array[Float], val _data: Array[Byte]) extends ValuesStore{

  def getBuilderType: StoreBuilderType = StoreBuilderAsSingleByteType

  def toTuple(): ValuesStoreAsSingleByte.TupleType = {
    (_numCols, minValues, maxValues, _data)
  }

  def fillRow(rowId: Int, output: Array[Double], isPos: Boolean): Unit = {
    if (!isPos){
      Utils.internalError()
    }
    FloatToSingleByteEncoder.decodeValues(rowId, minValues, maxValues,  rowId*_numCols, _data, output)
  }

  def setRow(rowId: Int, input: Array[Double]): Unit = {
    val offset = rowId*_numCols
    FloatToSingleByteEncoder.encodeValues(rowId, minValues, maxValues,  offset, _data, input)
  }


  def fillRow(rowId: Int, columnIds: Array[Int], output: Array[Double]): Unit = {
    val minValue = minValues(rowId)
    val maxValue = maxValues(rowId)
    val offset = rowId*_numCols
    var j = 0
    while(j < columnIds.length){
      val columnId = columnIds(j)
      val byteValue = _data(offset + columnId)
      output(columnId) = FloatToSingleByteEncoder.decodeValue(minValue, maxValue, byteValue)
      j += 1
    }
  }

  override def multiplyRowComponentWiseBySparseVector(rowId: Int, sv: SparseVector, output: Array[Double]): Unit = {
    val minValue = minValues(rowId)
    val maxValue = maxValues(rowId)

    val offset = rowId*_numCols
    val columnIds = sv.ids
    val values = sv.values
    val dim = columnIds.length
    var j = 0
    while(j < dim){
      val columnId = columnIds(j)
      val inputValue = values(j)
      val byteValue = _data(offset + columnId)
      val dataValue = FloatToSingleByteEncoder.decodeValue(minValue, maxValue, byteValue)
      output(j) = dataValue*inputValue
      j += 1
    }
  }

  def cosineForNormalizedData(query: Array[Double], id: Int): Double = {
    val minValue = minValues(id)
    val maxValue = maxValues(id)

    var offset = id*_numCols
    var j = 0
    var sum = 0.0
    while(j < _numCols){
      val v1 = query(j)
      val byteValue = _data(offset)
      val v2 = FloatToSingleByteEncoder.decodeValue(minValue, maxValue, byteValue)
      val d = v1 - v2
      sum += d*d
      offset += 1
      j += 1
    }
    1.0 - 0.5*sum
  }
}
trait StoreBuilderType{
  def getBuilder(numCols: Int): ValuesStoreBuilder
}
case object StoreBuilderAsDoubleType extends StoreBuilderType{
  def getBuilder(numCols: Int): ValuesStoreBuilder = new ValuesStoreBuilderAsDouble(numCols)
}
case object StoreBuilderAsBytesType extends StoreBuilderType{
  def getBuilder(numCols: Int): ValuesStoreBuilder = new ValuesStoreBuilderAsBytes(numCols)
}

case object StoreBuilderAsSingleByteType extends StoreBuilderType{
  def getBuilder(numCols: Int): ValuesStoreBuilder = new ValuesStoreBuilderAsSingleByte(numCols)
}

case class LazyLoadStoreBuilderType(backingDir: String, underlyingBuilder: StoreBuilderType) extends StoreBuilderType{
  if (underlyingBuilder.isInstanceOf[LazyLoadStoreBuilderType]){
    Utils.internalError()
  }

  def getBuilder(numCols: Int): ValuesStoreBuilder = new LazyLoadValuesStoreBuilder(backingDir, numCols, underlyingBuilder)
}

case class AsyncStoreBuilderType(backingDir: String, underlyingBuilder: StoreBuilderType) extends StoreBuilderType{
  if (underlyingBuilder.isInstanceOf[LazyLoadStoreBuilderType]){
    Utils.internalError()
  }

  def getBuilder(numCols: Int): ValuesStoreBuilder = new AsyncStoreBuilder(backingDir, numCols, underlyingBuilder)
}

/*
class StoreBuilderFactory(t: StoreBuilderType){
  def getBuilder(numCols: Int): ValuesStoreBuilder = {
    t.getBuilder(numCols)
  }
}
*/

object TestSingleByteEnc{
  def main(args: Array[String]) {
    val minV = -1.0f
    val maxV = 2.0f
    for(i <- 0 until 100) {
      val value = scala.util.Random.nextFloat()*3.0f - 1.0f
      val enc = FloatToSingleByteEncoder.encodeValue(minV, maxV, value)
      val dec = FloatToSingleByteEncoder.decodeValue(minV, maxV, enc)
      println("enc-dec-error: " + value + " " + dec + " " + (value - dec))
    }

  }
}