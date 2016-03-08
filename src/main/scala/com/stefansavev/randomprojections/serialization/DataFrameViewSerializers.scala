package com.stefansavev.randomprojections.serialization

import java.io.{InputStream, OutputStream}

import com.stefansavev.core.serialization.core.SubtypeSerializers._
import com.stefansavev.core.string2id.String2IdHasherSerialization
import String2IdHasherSerialization.String2IdHasherSerializer
import com.stefansavev.core.serialization.core.Utils._
import com.stefansavev.core.serialization.core.PrimitiveTypeSerializers._
import com.stefansavev.core.serialization.core.ImplicitSerializers._
import com.stefansavev.core.serialization.core.IsoSerializers._
import com.stefansavev.core.serialization.core.TupleSerializers._
import com.stefansavev.core.serialization.core.{Iso, TypedSerializer}
import com.stefansavev.core.string2id.String2IdHasher
import com.stefansavev.randomprojections.datarepr.dense._
import com.stefansavev.randomprojections.datarepr.dense.store._
import com.stefansavev.randomprojections.serialization.ColumnHeaderSerialization._

object DataFrameViewSerializers {

  implicit def valuesStoreSerializer(): TypedSerializer[ValuesStore] = {
    implicit object ValuesStoreAsDoubleSerializationTag extends TypeTag[ValuesStoreAsDouble]{
      def tag: Int = ValuesStoreAsDoubleSerializationTags.valuesStoreAsDouble
    }

    implicit object ValuesStoreAsBytesSerializationTag extends TypeTag[ValuesStoreAsBytes]{
      def tag: Int = ValuesStoreAsDoubleSerializationTags.valuesStoreAsBytes
    }

    implicit object ValuesStoreAsSingleByteSerializationTag extends TypeTag[ValuesStoreAsSingleByte]{
      def tag: Int = ValuesStoreAsDoubleSerializationTags.valuesStoreAsSingleByte
    }

    implicit object LazyLoadStoreSerializationTag extends TypeTag[LazyLoadValueStore]{
      def tag: Int = ValuesStoreAsDoubleSerializationTags.lazyLoadValuesStore
    }

    implicit object AsyncLoadStoreSerializationTag extends TypeTag[AsyncLoadValueStore]{
      def tag: Int = ValuesStoreAsDoubleSerializationTags.asyncLoadValuesStore
    }

    implicit def valuesStoreAsDoubleSerializer(): TypedSerializer[ValuesStoreAsDouble] = {

      implicit def valuesStoreTupleTypeSerializer(): TypedSerializer[ValuesStoreAsDouble.TupleType] = {
        tuple2Serializer[Int, Array[Double]](TypedIntSerializer, TypedDoubleArraySerializer)
      }

      implicit object ValuesStoreIso extends Iso[ValuesStoreAsDouble, ValuesStoreAsDouble.TupleType]{
        def from(input: Input): Output = input.toTuple
        def to(t: Output): Input = ValuesStoreAsDouble.fromTuple(t)
      }

      isoSerializer[ValuesStoreAsDouble, ValuesStoreAsDouble.TupleType](ValuesStoreIso, valuesStoreTupleTypeSerializer())
    }

    implicit def valuesStoreAsBytesSerializer(): TypedSerializer[ValuesStoreAsBytes] = {
      type T = ValuesStoreAsBytes
      type TT = ValuesStoreAsBytes.TupleType
      implicit def valuesStoreTupleTypeSerializer(): TypedSerializer[TT] = {
        tuple2Serializer[Int, Array[Short]](TypedIntSerializer, TypedShortArraySerializer)
      }

      implicit object ValuesStoreIso extends Iso[T, TT]{
        def from(input: Input): Output = input.toTuple
        def to(t: Output): Input = ValuesStoreAsBytes.fromTuple(t)
      }

      isoSerializer[T, TT](ValuesStoreIso, valuesStoreTupleTypeSerializer())
    }

    implicit def valuesStoreAsSingleByteSerializer(): TypedSerializer[ValuesStoreAsSingleByte] = {
      type T = ValuesStoreAsSingleByte
      type TT = ValuesStoreAsSingleByte.TupleType
      implicit def valuesStoreTupleTypeSerializer(): TypedSerializer[TT] = {
        tuple4Serializer[Int, Array[Float], Array[Float], Array[Byte]](TypedIntSerializer, TypedFloatArraySerializer, TypedFloatArraySerializer, TypedByteArraySerializer)
      }

      implicit object ValuesStoreIso extends Iso[T, TT]{
        def from(input: Input): Output = input.toTuple
        def to(t: Output): Input = ValuesStoreAsSingleByte.fromTuple(t)
      }

      isoSerializer[T, TT](ValuesStoreIso, valuesStoreTupleTypeSerializer())
    }

    implicit def lazyLoadStoreSerializer(): TypedSerializer[LazyLoadValueStore] = {
      type T = LazyLoadValueStore
      type TT = LazyLoadValueStore.TupleType

      implicit def valuesStoreTupleTypeSerializer(): TypedSerializer[TT] = {
        tuple5Serializer[String, Int, Int, Int, Int](TypedStringSerializer, TypedIntSerializer, TypedIntSerializer, TypedIntSerializer, TypedIntSerializer)
      }

      implicit object ValuesStoreIso extends Iso[T, TT]{
        def from(input: Input): Output = input.toTuple
        def to(t: Output): Input = LazyLoadValueStore.fromTuple(t)
      }

      isoSerializer[T, TT](ValuesStoreIso, valuesStoreTupleTypeSerializer())
    }

    implicit def asyncLoadStoreSerializer(): TypedSerializer[AsyncLoadValueStore] = {
      type T = AsyncLoadValueStore
      type TT = AsyncLoadValueStore.TupleType

      implicit def valuesStoreTupleTypeSerializer(): TypedSerializer[TT] = {
        tuple6Serializer[String, Array[Long], Int, Int, Int, Int](TypedStringSerializer, TypedLongArraySerializer, TypedIntSerializer, TypedIntSerializer, TypedIntSerializer, TypedIntSerializer)
      }

      implicit object ValuesStoreIso extends Iso[T, TT]{
        def from(input: Input): Output = input.toTuple
        def to(t: Output): Input = AsyncLoadValueStore.fromTuple(t)
      }

      isoSerializer[T, TT](ValuesStoreIso, valuesStoreTupleTypeSerializer())
    }

    val valueStoreSerializer = subtype5Serializer[ValuesStore,
                        ValuesStoreAsDouble,
                        ValuesStoreAsBytes,
                        ValuesStoreAsSingleByte,
                        LazyLoadValueStore,
                        AsyncLoadValueStore](
        ValuesStoreAsDoubleSerializationTag,
        ValuesStoreAsBytesSerializationTag,
        ValuesStoreAsSingleByteSerializationTag,
        LazyLoadStoreSerializationTag,
        AsyncLoadStoreSerializationTag,
        valuesStoreAsDoubleSerializer(),
        valuesStoreAsBytesSerializer(),
        valuesStoreAsSingleByteSerializer(),
        lazyLoadStoreSerializer(),
        asyncLoadStoreSerializer())

    object ValueStoreWithTransformedTypeSerializer extends TypedSerializer[ValuesStore] {
      def toBinary(outputStream: OutputStream, input: ValuesStore): Unit = {
        valueStoreSerializer.toBinary(outputStream, input)
      }

      def fromBinary(inputStream: InputStream): ValuesStore = {
        val vs = valueStoreSerializer.fromBinary(inputStream)
        vs match {
          case lazyLoadStore: LazyLoadValueStore => {
            lazyLoadStore.loadAll()
          }
          case asyncLoadStore: AsyncLoadValueStore => {
            asyncLoadStore.loadAll()
          }
          case _ => vs
        }
      }
    }
    ValueStoreWithTransformedTypeSerializer
  }

  implicit object DenseRowStoredMatrixViewIso extends Iso[DenseRowStoredMatrixView, DenseRowStoredMatrixView.TupleType]{
    def from(input: Input): Output = input.toTuple
    def to(t: Output): Input = DenseRowStoredMatrixView.fromTuple(t)
  }

  implicit object DenseRowStoredMatrixViewTag extends TypeTag[DenseRowStoredMatrixView]{
    def tag: Int = DenseRowStoredMatrixView.tag
  }

  //this is how to help the compiler
  implicit def denseRowStoredMatrixViewTupleTypeSerializer(): TypedSerializer[DenseRowStoredMatrixView.TupleType] = {
    tuple5Serializer[Int, ValuesStore, Array[Int], ColumnHeader, String2IdHasher](TypedIntSerializer, valuesStoreSerializer(), TypedIntArraySerializer, ColumnHeaderSerializer, String2IdHasherSerializer)
  }

  implicit def denseRowStoredMatrixSerializer(): TypedSerializer[DenseRowStoredMatrixView] = {
    isoSerializer[DenseRowStoredMatrixView, DenseRowStoredMatrixView.TupleType](DenseRowStoredMatrixViewIso, denseRowStoredMatrixViewTupleTypeSerializer())
  }

  implicit def rowStoredMatrixSerializer(): TypedSerializer[RowStoredMatrixView] = {
    subtype1Serializer[RowStoredMatrixView, DenseRowStoredMatrixView](DenseRowStoredMatrixViewTag, denseRowStoredMatrixSerializer())
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

  implicit def dataFrameViewTupleSerializer(): TypedSerializer[DataFrameView.TupleType] = {
    tuple2Serializer[PointIndexes, RowStoredMatrixView](pointIndexesSerializer, rowStoredMatrixSerializer)
  }

  implicit def dataFrameSerializer(): TypedSerializer[DataFrameView] = {
    isoSerializer[DataFrameView, DataFrameView.TupleType](DataFrameViewIso, dataFrameViewTupleSerializer())
  }

}
