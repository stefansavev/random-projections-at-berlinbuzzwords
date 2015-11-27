package com.stefansavev.randomprojections.serialization

import java.io.OutputStream

import com.stefansavev.randomprojections.datarepr.dense.{ColumnHeaderImpl, ColumnHeader}
import com.stefansavev.randomprojections.serialization.core.PrimitiveTypeSerializers.TypedIntSerializer
import com.stefansavev.randomprojections.serialization.core.TypedSerializer

object ColumnHeaderSerialization{
  implicit object ColumnHeaderSerializer extends TypedSerializer[ColumnHeader]{
    import ImplicitSerializers._
    def toBinary(stream: OutputStream, header: ColumnHeader): Unit = {
      stream.writeInt(header.numCols)
    }

    def fromBinary(stream: java.io.InputStream): ColumnHeader = {
      val numCols = stream.readInt()
      new ColumnHeaderImpl(numCols, null, null, null, false)
    }

    def sizeInBytes(input: ColumnHeader): Long = {
      TypedIntSerializer.sizeInBytes(input.numCols)
    }
  }
}

