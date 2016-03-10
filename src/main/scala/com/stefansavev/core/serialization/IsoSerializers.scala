package com.stefansavev.core.serialization

import java.io.{InputStream, OutputStream}

object IsoSerializers {
  class IsoSerializer[A, B](iso: Iso[A, B], serB: TypedSerializer[B]) extends TypedSerializer[A]{
    def toBinary(outputStream: OutputStream, input: A): Unit = {
      serB.toBinary(outputStream, iso.from(input))
    }

    def fromBinary(inputStream: InputStream): A = {
      iso.to(serB.fromBinary(inputStream))
    }

    def name: String = s"IsoSerializer(via = ${serB.name})"
  }

  implicit def isoSerializer[A, B](implicit iso: Iso[A, B], serB: TypedSerializer[B]): IsoSerializer[A, B] = {
    new IsoSerializer[A, B](iso, serB)
  }
}
