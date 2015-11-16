package com.stefansavev.randomprojections.datarepr.dense

trait RowStoredMatrixViewBuilder{
  def currentRowId: Int
  def addRow(label: Int, ids: Array[Int], values: Array[Double]): Unit
  def addRow(name: String, label: Int, ids: Array[Int], values: Array[Double]): Unit
  def build(): RowStoredMatrixView
}

