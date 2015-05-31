package com.stefansavev.randomprojections.datarepr.dense

import com.stefansavev.randomprojections.datarepr.sparse.SparseVector

case class PointIndexes(indexes: Array[Int]){
  def size = indexes.length
  def apply(i: Int): Int = indexes(i)
}

class DataFrameView(val indexes: PointIndexes, val rowStoredView: RowStoredMatrixView) {
  def numRows: Int = indexes.size
  def numCols: Int = rowStoredView.numCols


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

