package com.stefansavev.randomprojections.datarepr.dense.store

import com.stefansavev.randomprojections.buffers.{ByteArrayBuffer, FloatArrayBuffer}
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.utils.Utils

object ValuesStoreAsSingleByte {
  type TupleType = (Int, Array[Float], Array[Float], Array[Byte])

  def fromTuple(t: TupleType): ValuesStoreAsSingleByte = {
    val (numCols, minValues, maxValues, data) = t
    new ValuesStoreAsSingleByte(numCols, minValues, maxValues, data)
  }
}

class ValuesStoreAsSingleByte(val _numCols: Int, val minValues: Array[Float], val maxValues: Array[Float], val _data: Array[Byte]) extends ValuesStore {

  def getBuilderType: StoreBuilderType = StoreBuilderAsSingleByteType

  def toTuple(): ValuesStoreAsSingleByte.TupleType = {
    (_numCols, minValues, maxValues, _data)
  }

  def fillRow(rowId: Int, output: Array[Double], isPos: Boolean): Unit = {
    if (!isPos) {
      Utils.internalError()
    }
    FloatToSingleByteEncoder.decodeValues(rowId, minValues, maxValues, rowId * _numCols, _data, output)
  }

  def setRow(rowId: Int, input: Array[Double]): Unit = {
    val offset = rowId * _numCols
    FloatToSingleByteEncoder.encodeValues(rowId, minValues, maxValues, offset, _data, input)
  }


  def fillRow(rowId: Int, columnIds: Array[Int], output: Array[Double]): Unit = {
    val minValue = minValues(rowId)
    val maxValue = maxValues(rowId)
    val offset = rowId * _numCols
    var j = 0
    while (j < columnIds.length) {
      val columnId = columnIds(j)
      val byteValue = _data(offset + columnId)
      output(columnId) = FloatToSingleByteEncoder.decodeValue(minValue, maxValue, byteValue)
      j += 1
    }
  }

  override def multiplyRowComponentWiseBySparseVector(rowId: Int, sv: SparseVector, output: Array[Double]): Unit = {
    val minValue = minValues(rowId)
    val maxValue = maxValues(rowId)

    val offset = rowId * _numCols
    val columnIds = sv.ids
    val values = sv.values
    val dim = columnIds.length
    var j = 0
    while (j < dim) {
      val columnId = columnIds(j)
      val inputValue = values(j)
      val byteValue = _data(offset + columnId)
      val dataValue = FloatToSingleByteEncoder.decodeValue(minValue, maxValue, byteValue)
      output(j) = dataValue * inputValue
      j += 1
    }
  }

  def cosineForNormalizedData(query: Array[Double], id: Int): Double = {
    val minValue = minValues(id)
    val maxValue = maxValues(id)

    var offset = id * _numCols
    var j = 0
    var sum = 0.0
    while (j < _numCols) {
      val v1 = query(j)
      val byteValue = _data(offset)
      val v2 = FloatToSingleByteEncoder.decodeValue(minValue, maxValue, byteValue)
      val d = v1 - v2
      sum += d * d
      offset += 1
      j += 1
    }
    1.0 - 0.5 * sum
  }
}

class ValuesStoreBuilderAsSingleByte(numCols: Int) extends ValuesStoreBuilder {
  //val minExponentPerRecord = new IntArrayBuffer()
  //val maxExponentPerRecord = new IntArrayBuffer()

  val minValuePerRecord = new FloatArrayBuffer()
  val maxValuePerRecord = new FloatArrayBuffer()
  //val rangeSizeInBits = new ByteArrayBuffer()

  val valuesBuffer = new ByteArrayBuffer()
  var currentRow = 0

  def allBits(i: Int): String = {
    val p = i.toBinaryString
    val extra = (32 - p.size)
    Array.range(0, extra).map(_ => "0").mkString("") + p
  }

  def decodeEncodeTestExp(value: Double): Unit = {
    val exp = FloatToSingleByteEncoder.getExponent(value) - 127 //the exponent is biased

    if (value > 0) {
      val logValue = (Math.log(value) / Math.log(2.0)).floor
      //println("+exp: " + exp + " " + logValue + " diff: " + (exp - logValue))
    }
    else {
      val logValue = (Math.log(-value) / Math.log(2.0)).floor
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
    if (values.length != numCols) {
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

    val minValuePerRecord = new FixedLengthBuffer[Float](numTotalRows)
    val maxValuePerRecord = new FixedLengthBuffer[Float](numTotalRows)
    val valuesBuffer = new FixedLengthBuffer[Byte](numTotalRows * numCols)
    for (store <- valueStores) {
      val typedStore = store.asInstanceOf[ValuesStoreAsSingleByte]
      minValuePerRecord ++= typedStore.minValues
      maxValuePerRecord ++= typedStore.maxValues
      valuesBuffer ++= typedStore._data
      if (typedStore._numCols != this.numCols) {
        Utils.internalError()
      }
    }
    new ValuesStoreAsSingleByte(this.numCols, minValuePerRecord.array, maxValuePerRecord.array, valuesBuffer.array)
  }

  def isFull: Boolean = Utils.internalError()

  def getContents(): Array[Byte] = Utils.internalError()
}
