package com.stefansavev.randomprojections.datarepr.dense

import com.stefansavev.randomprojections.buffers.{DoubleArrayBuffer, IntArrayBuffer}
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.file.CSVFileOptions

class DenseRowStoredMatrixView(_numCols: Int, val data: Array[Double], val labels: Array[Int], header: ColumnHeader) extends RowStoredMatrixView{

  def toTuple: DenseRowStoredMatrixView.TupleType = (_numCols, data, labels, header)

  override def numCols: Int = _numCols

  override def getColumnHeader: ColumnHeader = header
  override def getAllRowNames(): Array[String] = header.getRowNames

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

  type TupleType = (Int, Array[Double], Array[Int], ColumnHeader)

  def fromTuple(t: DenseRowStoredMatrixView.TupleType): DenseRowStoredMatrixView = {
    new DenseRowStoredMatrixView(t._1, t._2, t._3, t._4)
  }

  def tag: Int = 1

}

object DenseRowStoredMatrixViewBuilderFactory extends RowStoredMatrixViewBuilderFactory{
  def create(header: ColumnHeader): RowStoredMatrixViewBuilder = new DenseRowStoredMatrixViewBuilder(header)
}

class DenseRowStoredMatrixViewBuilder(header: ColumnHeader) extends RowStoredMatrixViewBuilder{
  val numCols = header.numCols

  var _currentRow = 0

  val valuesBuffer = new DoubleArrayBuffer()
  val labelsBuilder = new IntArrayBuffer()

  override def currentRowId: Int = _currentRow

  override def addRow(label: Int, ids: Array[Int], values: Array[Double]): Unit = {
    val sz = ids.length
    if (sz != numCols){
      throw new IllegalStateException("ids.length should equal number of columns")
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
    new DenseRowStoredMatrixView(numCols, valuesBuffer.toArray, labelsBuilder.toArray, header)
  }

  def build(newHeader: ColumnHeader): RowStoredMatrixView = {
    new DenseRowStoredMatrixView(numCols, valuesBuffer.toArray, labelsBuilder.toArray, newHeader)
  }


}

