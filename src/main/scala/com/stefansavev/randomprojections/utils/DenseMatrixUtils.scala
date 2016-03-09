package com.stefansavev.randomprojections.utils

import no.uib.cipr.matrix.DenseMatrix

object DenseMatrixUtils {
  def addToRow(dm: DenseMatrix, row: Int, vec: Array[Double]): Unit = {
    if (dm.numColumns() != vec.length) {
      Utils.internalError()
    }
    val len = vec.length
    var i = 0
    while (i < len) {
      val prevValue = dm.get(row, i)
      dm.set(row, i, prevValue + vec(i))
      i += 1
    }
  }

  def takeRows(dm: DenseMatrix, topK: Int): DenseMatrix = {
    val partition = new DenseMatrix(topK, dm.numColumns())
    var i = 0
    while (i < topK) {
      var j = 0
      while (j < dm.numColumns()) {
        partition.set(i, j, dm.get(i, j))
        j += 1
      }
      i += 1
    }
    partition
  }

  def addMatrixTo(modifiedMatrix: DenseMatrix, increment: DenseMatrix): Unit = {
    if (modifiedMatrix.numRows() != increment.numRows()) {
      Utils.internalError()
    }
    if (modifiedMatrix.numColumns() != increment.numColumns()) {
      Utils.internalError()
    }
    var i = 0
    while (i < modifiedMatrix.numRows()) {
      var j = 0
      while (j < modifiedMatrix.numColumns()) {
        val prevValue = modifiedMatrix.get(i, j)
        val newValue = prevValue + increment.get(i, j)
        modifiedMatrix.set(i, j, newValue)
        j += 1
      }
      i += 1
    }
  }
}
