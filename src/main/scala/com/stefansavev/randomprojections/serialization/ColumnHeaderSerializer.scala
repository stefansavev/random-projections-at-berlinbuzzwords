package com.stefansavev.randomprojections.serialization

import java.io.OutputStream

import com.stefansavev.core.serialization.core.PrimitiveTypeSerializers.TypedIntSerializer
import com.stefansavev.core.serialization.core.{ImplicitSerializers, TypedSerializer}
import com.stefansavev.randomprojections.datarepr.dense.{ColumnHeader, ColumnHeaderImpl}

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
  }
}

