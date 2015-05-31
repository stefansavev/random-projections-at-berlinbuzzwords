package com.stefansavev.randomprojections.datarepr.dense

trait RowStoredMatrixViewBuilderFactory{
  def create(header: ColumnHeader): RowStoredMatrixViewBuilder
}

object RowStoredMatrixViewBuilderFactory {

  def createDense(header: ColumnHeader): RowStoredMatrixViewBuilder = {
    new DenseRowStoredMatrixViewBuilder(header)
  }
}
