package com.stefansavev.randomprojections.serialization

import java.io.{InputStream, OutputStream}

import com.stefansavev.randomprojections.datarepr.dense._
import com.stefansavev.randomprojections.serialization.core.Core._
import com.stefansavev.randomprojections.serialization.core.TupleSerializers._
import com.stefansavev.randomprojections.serialization.core.PrimitiveTypeSerializers._
import com.stefansavev.randomprojections.serialization.ColumnHeaderSerialization._
import com.stefansavev.randomprojections.serialization.core.TypedSerializer

object DataFrameViewSerializers {

  implicit object DenseRowStoredMatrixViewIso extends Iso[DenseRowStoredMatrixView, DenseRowStoredMatrixView.TupleType]{
    def from(input: Input): Output = input.toTuple
    def to(t: Output): Input = DenseRowStoredMatrixView.fromTuple(t)
  }

  implicit object DenseRowStoredMatrixViewTag extends TypeTag[DenseRowStoredMatrixView]{
    def tag: Int = DenseRowStoredMatrixView.tag
  }
  //this is how to help the compiler
  //implicit def denseRowStoredMatrixViewTupleTypeSerializer(): TypedSerializer[DenseRowStoredMatrixView.TupleType] = {
  //  tuple4Serializer[Int, Array[Double], Array[Int], ColumnHeader]
  //}

  implicit def denseRowStoredMatrixSerializer(): TypedSerializer[DenseRowStoredMatrixView] = {
    isoSerializer[DenseRowStoredMatrixView, DenseRowStoredMatrixView.TupleType]
  }


  implicit def rowStoredMatrixSerializer(): TypedSerializer[RowStoredMatrixView] = {
    subtype1Serializer[RowStoredMatrixView, DenseRowStoredMatrixView]
  }

  implicit object PointIndexesIso extends Iso[PointIndexes, PointIndexes.TupleType]{
    def from(input: Input): Output = input.toTuple
    def to(t: Output): Input = PointIndexes.fromTuple(t)
  }

  implicit def pointIndexesSerializer(): TypedSerializer[PointIndexes] = {
    isoSerializer[PointIndexes, PointIndexes.TupleType]
  }

  implicit object DataFrameViewIso extends Iso[DataFrameView, DataFrameView.TupleType] {
    def from(input: Input): Output = input.toTuple
    def to(t: Output): Input = DataFrameView.fromTuple(t)
  }

  implicit def testSerializer(): TypedSerializer[DataFrameView.TupleType] = {
    tuple2Serializer[PointIndexes, RowStoredMatrixView](pointIndexesSerializer, rowStoredMatrixSerializer)
  }

  implicit def dataFrameSerializer(): TypedSerializer[DataFrameView] = {
    isoSerializer[DataFrameView, DataFrameView.TupleType](DataFrameViewIso, testSerializer())
  }

  //
  // class DenseRowStoredMatrixView(_numCols: Int, val data: Array[Double], val labels: Array[Int], header: ColumnHeader) extends RowStoredMatrixView{
}
