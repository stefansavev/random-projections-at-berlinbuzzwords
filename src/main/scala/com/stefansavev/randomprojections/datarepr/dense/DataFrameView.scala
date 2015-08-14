package com.stefansavev.randomprojections.datarepr.dense

import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.implementation.PointSignatures

class PointIndexes(val indexes: Array[Int]){
  def toTuple: PointIndexes.TupleType = (0, indexes)
  def size = indexes.length
  def apply(i: Int): Int = indexes(i)
}

object PointIndexes{
  type TupleType = (Int, Array[Int]) //the first is dummy because I need to add a tuple1
  def apply(indexes: Array[Int]): PointIndexes = new PointIndexes(indexes)
  def unapply(pntIndexes: PointIndexes): Option[Array[Int]] = Some(pntIndexes.indexes)
  def fromTuple(t: TupleType): PointIndexes = new PointIndexes(t._2)
}

class DataFrameView(val indexes: PointIndexes, val rowStoredView: RowStoredMatrixView) {
  def toTuple:DataFrameView.TupleType = (indexes, rowStoredView)

  var pointSignatures: PointSignatures = null //work around until the concept is validated
  def numRows: Int = indexes.size
  def numCols: Int = rowStoredView.numCols

  def setPointSignatures(pointSignatures: PointSignatures): Unit = {
    this.pointSignatures = pointSignatures
  }

  def getPointSignatures(): PointSignatures = {
    this.pointSignatures
  }

  def getPointAsDenseVector(pntId: Int): Array[Double] = {
    rowStoredView.getPointAsDenseVector(pntId)
  }

  def getPointAsDenseVector(pntId: Int, columnIds: Array[Int], vec: Array[Double]): Unit = {
    rowStoredView.getPointAsDenseVector(pntId, columnIds, vec)
  }

  def multiplyRowComponentWiseBySparseVector(pntId: Int, sv: SparseVector, output: Array[Double]): Unit = {
    rowStoredView.multiplyRowComponentWiseBySparseVector(pntId, sv, output)
  }

  def getUnderlyingIndexes(): PointIndexes = indexes

  def childView(newIndexes: PointIndexes): DataFrameView = {
    new DataFrameView(newIndexes, rowStoredView)
  }

  def getLabel(rowId: Int): Int = rowStoredView.getLabel(rowId)

  def getAllLabels(): Array[Int] = rowStoredView.getAllLabels()

  def dist(id1: Int, id2: Int): Double = rowStoredView.dist(id1, id2)

  def cosineForNormalizedData(query: Array[Double], id: Int): Double = rowStoredView.cosineForNormalizedData(query, id)

  override def toString = s"DataFrameView($numRows, $numCols)"
}

object DataFrameView{
  type TupleType = (PointIndexes, RowStoredMatrixView)
  def fromTuple(t: TupleType) = new DataFrameView(t._1, t._2)
}

