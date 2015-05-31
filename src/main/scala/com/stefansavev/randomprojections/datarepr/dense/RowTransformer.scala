package com.stefansavev.randomprojections.datarepr.dense

trait RowTransformer{
  def transformToDouble(line: Int, column: Int, value: String): Double
  def transformToInt(line: Int, column: Int, value: String): Int
}

trait RowTransformerBuilder{
  def getTransformer(name: String): Option[RowTransformer]
}

object NoRowTransformerBuilderProvided extends RowTransformerBuilder{
  override def getTransformer(name: String): Option[RowTransformer] = None
}

class IntTransformer(columnName: String) extends RowTransformer{
  def transformToDouble(line: Int, column: Int, value: String): Double = throw new IllegalStateException("value should not be converted to double")
  def transformToInt(line: Int, column: Int, value: String): Int = {
    val value1 = if (value.endsWith(".0")) value.substring(0, value.length - 2) else value //workaround for wrong format

    value1.toInt
  }
}

class DoubleTransformer(columnName: String) extends RowTransformer{
  def transformToDouble(line: Int, column: Int, value: String): Double = value.toDouble
  def transformToInt(line: Int, column: Int, value: String): Int = throw new IllegalStateException("value should not be converted to int")
}
