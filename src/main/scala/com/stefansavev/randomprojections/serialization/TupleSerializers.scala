package com.stefansavev.randomprojections.serialization

import java.io._

object TupleSerializers {

  class Tuple2Serializer[A1, A2](serA1: TypedSerializer[A1], serA2: TypedSerializer[A2]) extends TypedSerializer[(A1, A2)] {

    def toBinary(outputStream: OutputStream, input: (A1, A2)): Unit = {
      serA1.toBinary(outputStream, input._1)
      serA2.toBinary(outputStream, input._2)
    }

    def fromBinary(inputStream: InputStream): (A1, A2) = {
      val a1 = serA1.fromBinary(inputStream)
      val a2 = serA2.fromBinary(inputStream)
      (a1, a2)
    }
  }

  implicit def tuple2Serializer[A1, A2](implicit serA1: TypedSerializer[A1], serA2: TypedSerializer[A2]): Tuple2Serializer[A1, A2] = {
    new Tuple2Serializer[A1, A2](serA1, serA2)
  }


  //---------------------------------------------------------
}

