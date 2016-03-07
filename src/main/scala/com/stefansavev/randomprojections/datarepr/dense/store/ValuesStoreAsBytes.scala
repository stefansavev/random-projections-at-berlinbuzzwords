package com.stefansavev.randomprojections.datarepr.dense.store

import com.stefansavev.randomprojections.buffers.ShortArrayBuffer
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.utils.Utils

object ValuesStoreAsBytes {
  type TupleType = (Int, Array[Short])

  def fromTuple(t: TupleType): ValuesStoreAsBytes = {
    val (numCols, data) = t
    new ValuesStoreAsBytes(numCols, data)
  }
}

class ValuesStoreAsBytes(val _numCols: Int, val _data: Array[Short]) extends ValuesStore {

  def getBuilderType: StoreBuilderType = StoreBuilderAsBytesType

  def getAtIndex(index: Int): Double = {
    FloatToByteEncoder.decodeValue(_data(index))
  }

  def toTuple(): ValuesStoreAsBytes.TupleType = {
    (_numCols, _data)
  }

  def fillRow(rowId: Int, output: Array[Double], isPos: Boolean): Unit = {
    var offset = rowId * _numCols
    var j = 0
    while (j < _numCols) {
      if (isPos) {
        output(j) += getAtIndex(offset)
      } else {
        output(j) -= getAtIndex(offset)
      }
      offset += 1
      j += 1
    }
  }

  def setRow(rowId: Int, input: Array[Double]): Unit = {
    var offset = rowId * _numCols
    var j = 0
    while (j < _numCols) {
      _data(offset) = FloatToByteEncoder.encodeValue(input(j))
      offset += 1
      j += 1
    }
  }


  def fillRow(rowId: Int, columnIds: Array[Int], output: Array[Double]): Unit = {
    val offset = rowId * _numCols
    var j = 0
    while (j < columnIds.length) {
      val columnId = columnIds(j)
      output(columnId) = getAtIndex(offset + columnId)
      j += 1
    }
  }

  override def multiplyRowComponentWiseBySparseVector(rowId: Int, sv: SparseVector, output: Array[Double]): Unit = {
    val offset = rowId * _numCols
    val columnIds = sv.ids
    val values = sv.values
    val dim = columnIds.length
    var j = 0
    while (j < dim) {
      val columnId = columnIds(j)
      val value = values(j)
      output(j) = getAtIndex(offset + columnId) * value
      j += 1
    }
  }

  def cosineForNormalizedData(query: Array[Double], id: Int): Double = {
    var offset = id * _numCols
    var j = 0
    var sum = 0.0
    while (j < _numCols) {
      val v1 = query(j)
      val v2 = getAtIndex(offset)
      val d = v1 - v2
      sum += d * d
      offset += 1
      j += 1
    }
    1.0 - 0.5 * sum
  }
}

class ValuesStoreBuilderAsBytes(numCols: Int) extends ValuesStoreBuilder {
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
    if (values.length != numCols) {
      Utils.internalError()
    }
    var i = 0
    while (i < values.length) {
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
    for (store <- valueStores) {
      val typedStore = store.asInstanceOf[ValuesStoreAsBytes]
      buffer ++= typedStore._data
      if (numColumns >= 0 && typedStore._numCols != numColumns) {
        Utils.internalError()
      }
      numColumns = typedStore._numCols
    }
    if (buffer.array.length != buffer.size) {
      Utils.internalError()
    }
    new ValuesStoreAsBytes(numColumns, buffer.array)
  }

  def isFull: Boolean = Utils.internalError()

  def getContents(): Array[Byte] = Utils.internalError()
}