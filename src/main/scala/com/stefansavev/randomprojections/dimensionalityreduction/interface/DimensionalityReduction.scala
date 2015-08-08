package com.stefansavev.randomprojections.dimensionalityreduction.interface

import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.dimensionalityreduction.svd.{SVDTransform, SVD, SVDParams}


trait DimensionalityReductionParams{
}

trait DimensionalityReductionTransform{
}

case class DimensionalityReductionResult(transform: DimensionalityReductionTransform, transformedDataset: DataFrameView)

//need to save the projection vectors
object DimensionalityReduction {
  def fit(params: DimensionalityReductionParams, dataFrame: DataFrameView): DimensionalityReductionTransform = {
    params match {
      case svdParams: SVDParams => SVD.fit(svdParams, dataFrame)
    }
  }

  def fitTransform(params: DimensionalityReductionParams, dataFrame: DataFrameView): DimensionalityReductionResult = {
    val computedTransform = fit(params, dataFrame)
    val output = transform(computedTransform, dataFrame)
    DimensionalityReductionResult(computedTransform, output)
  }

  def transform(transformParam: DimensionalityReductionTransform, inputData: DataFrameView): DataFrameView = {
    transformParam match {
      case svdTransform: SVDTransform => {
        SVD.transform(svdTransform, inputData)
      }
    }
  }
}
