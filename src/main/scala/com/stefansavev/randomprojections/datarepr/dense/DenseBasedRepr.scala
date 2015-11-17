package com.stefansavev.randomprojections.datarepr.dense

import com.stefansavev.randomprojections.buffers.{DoubleArrayBuffer, IntArrayBuffer}
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.file.CSVFileOptions
import com.stefansavev.randomprojections.utils.{StringIdHasherSettings, String2IdHasher}

class DenseRowStoredMatrixView(_numCols: Int, val data: Array[Double], val labels: Array[Int], header: ColumnHeader, rowName2Hash: String2IdHasher = null) extends RowStoredMatrixView{

  def toTuple: DenseRowStoredMatrixView.TupleType = (_numCols, data, labels, header, rowName2Hash)

  override def numCols: Int = _numCols

  override def getColumnHeader: ColumnHeader = header

  override def getAllRowNames(): Array[String] = {
    val len = rowName2Hash.numberOfUniqueStrings()
    val output = Array.ofDim[String](len)
    var i = 0
    while(i < len){
      output(i) = rowName2Hash.getStringAtInternalIndex(i).get
      i += 1
    }
    output
  }

  def getRowIdByName(name: String): Int = {
    val hashCode = rowName2Hash.getOrAddId(name, false)
    if (hashCode < 0){
      -1
    }
    else{
      rowName2Hash.getInternalId(hashCode)
    }
  }

  def getName(rowId: Int): String = {
    if (rowName2Hash != null){
      rowName2Hash.getStringAtInternalIndex(rowId).get
    }
    else{
      null
    }
  }

  def getAllLabels(): Array[Int] = labels

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

  def fillRows(rowIds: Array[Int], output: Array[Double], isPos: Boolean): Unit = {
    var i = 0
    while(i < rowIds.length) {
      val rowId = rowIds(i)
      var offset = rowId * _numCols
      var j = 0
      while (j < _numCols) {
        if (isPos) {
          output(j) += data(offset)
        } else {
          output(j) -= data(offset)
        }
        offset += 1
        j += 1
      }
      i += 1
    }
  }

  override def getPointAsDenseVector(pntId: Int): Array[Double] = {
    val output = Array.ofDim[Double](_numCols)
    fillRow(pntId, output, true)
    output
  }

  override def getPointAsDenseVector(pntId: Int, columnIds: Array[Int], output: Array[Double]) = {
    fillRow(pntId, columnIds, output)
  }

  override def dist(id1: Int, id2: Int): Double = {
    var offset1 = id1*_numCols
    var offset2 = id2*_numCols

    var j = 0
    var sum = 0.0
    while(j < _numCols){
      val v1 = data(offset1)
      val v2 = data(offset2)
      val d = v1 - v2
      sum += d*d
      offset1 += 1
      offset2 += 1
      j += 1
    }
    sum
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

  def getLabel(rowId: Int): Int = labels(rowId)
}

object DenseRowStoredMatrixView{

  def fromFile(fileName: String,  opt: CSVFileOptions, dataFrameOptions: DataFrameOptions): DataFrameView = {
    RowStoredMatrixView.fromFile(fileName, opt, dataFrameOptions)
  }

  type TupleType = (Int, Array[Double], Array[Int], ColumnHeader, String2IdHasher)

  def fromTuple(t: DenseRowStoredMatrixView.TupleType): DenseRowStoredMatrixView = {
    new DenseRowStoredMatrixView(t._1, t._2, t._3, t._4, t._5)
  }

  def tag: Int = 1

}

object DenseRowStoredMatrixViewBuilderFactory extends RowStoredMatrixViewBuilderFactory{
  def create(header: ColumnHeader): RowStoredMatrixViewBuilder = new DenseRowStoredMatrixViewBuilder(header)
}

class DenseRowStoredMatrixViewBuilder(header: ColumnHeader) extends RowStoredMatrixViewBuilder{
  val maxRows: Int = 2 << 19 //maximum number of rows is just above 3 million
  val string2IdHasher = if (header.hasRowNames()) new String2IdHasher(StringIdHasherSettings(maxRows, 50, 4)) else null
  val numCols = header.numCols

  var _currentRow = 0

  val valuesBuffer = new DoubleArrayBuffer()
  val labelsBuilder = new IntArrayBuffer()

  override def currentRowId: Int = _currentRow

  override def addRow(label: Int, ids: Array[Int], values: Array[Double]): Unit = {
    addRow(null, label, ids, values)
  }

  override def addRow(name: String, label: Int, ids: Array[Int], values: Array[Double]): Unit = {
    val sz = ids.length
    if (sz != numCols){
      throw new IllegalStateException("ids.length should equal number of columns")
    }

    if (string2IdHasher != null) {
      val hashCode = string2IdHasher.add(name)
      if (hashCode < 0) {
        throw new IllegalStateException("max. number of allowed documents is exceeded")
      }
      val internalIndex = string2IdHasher.getInternalId(hashCode)
      if (internalIndex != _currentRow) {
        throw new IllegalStateException("Internal error: internalIndex != _currentRow")
      }
    }

    val orderedValues = Array.ofDim[Double](sz)
    var i = 0
    while(i < ids.length){
      val id = ids(i)
      if (id != i){
        throw new IllegalStateException("ids should be consecutive")
      }
      val value = values(i)
      orderedValues(id) = value
      i += 1
    }

    labelsBuilder += label
    valuesBuffer ++= orderedValues
    _currentRow += 1
  }

  override def build(): RowStoredMatrixView = {
    new DenseRowStoredMatrixView(numCols, valuesBuffer.toArray, labelsBuilder.toArray, header, string2IdHasher)
  }

  def build(newHeader: ColumnHeader): RowStoredMatrixView = {
    new DenseRowStoredMatrixView(numCols, valuesBuffer.toArray, labelsBuilder.toArray, newHeader)
  }
}

