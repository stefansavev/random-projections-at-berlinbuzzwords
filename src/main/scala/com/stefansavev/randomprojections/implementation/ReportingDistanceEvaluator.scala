package com.stefansavev.randomprojections.implementation

import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.utils.Utils

trait ReportingDistanceEvaluator {
  def cosine(query: Array[Double], pointId: Int): Double
}

trait ReportingDistanceEvaluatorBuilder{
  type T <: ReportingDistanceEvaluator
  def build(origDataset: DataFrameView): T
}

class CosineOnOriginalDataDistanceEvaluator(origDataset: DataFrameView) extends ReportingDistanceEvaluator{
  def cosine(query: Array[Double], pointId: Int): Double = {
    origDataset.cosineForNormalizedData(query, pointId)
  }
}

class CosineOnOriginalDataDistanceEvaluatorBuilder extends ReportingDistanceEvaluatorBuilder{
  type T = CosineOnOriginalDataDistanceEvaluator

  def build(origDataset: DataFrameView): T = {
    new CosineOnOriginalDataDistanceEvaluator(origDataset)
  }

}

class SignatureDistanceEvaluator(origDataset: DataFrameView) extends ReportingDistanceEvaluator{
  def cosine(query: Array[Double], pointId: Int): Double = {
    Utils.todo()
  }
}

object ReportingDistanceEvaluators{
  def cosineOnOriginalData(): CosineOnOriginalDataDistanceEvaluatorBuilder = {
    new CosineOnOriginalDataDistanceEvaluatorBuilder()
  }
}


