package com.stefansavev.core.serialization

import java.io.{InputStream, OutputStream}

object PrimitiveTypeSerializers {

  implicit object TypedByteSerializer extends TypedSerializer[Byte] {
    def toBinary(outputStream: OutputStream, input: Byte): Unit = {
      ByteSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): Byte = {
      ByteSerializer.read(inputStream)
    }

    def name: String = "byte"
  }

  implicit object TypedIntSerializer extends TypedSerializer[Int] {
    def toBinary(outputStream: OutputStream, input: Int): Unit = {
      IntSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): Int = {
      IntSerializer.read(inputStream)
    }

    def name: String = "int"
  }

  implicit object TypedShortSerializer extends TypedSerializer[Short] {
    def toBinary(outputStream: OutputStream, input: Short): Unit = {
      ShortSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): Short = {
      ShortSerializer.read(inputStream)
    }

    def name: String = "short"
  }

  implicit object TypedLongSerializer extends TypedSerializer[Long] {
    def toBinary(outputStream: OutputStream, input: Long): Unit = {
      LongSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): Long = {
      LongSerializer.read(inputStream)
    }

    def name: String = "long"
  }

  implicit object TypedFloatSerializer extends TypedSerializer[Float] {
    def toBinary(outputStream: OutputStream, input: Float): Unit = {
      FloatSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): Float = {
      FloatSerializer.read(inputStream)
    }

    def name: String = "float"
  }

  implicit object TypedStringSerializer extends TypedSerializer[String] {
    def toBinary(outputStream: OutputStream, input: String): Unit = {
      StringSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): String = {
      StringSerializer.read(inputStream)
    }

    def name: String = "string"
  }

  implicit object TypedDoubleSerializer extends TypedSerializer[Double] {
    def toBinary(outputStream: OutputStream, input: Double): Unit = {
      DoubleSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): Double = {
      DoubleSerializer.read(inputStream)
    }

    def name: String = "double"
  }

  implicit object TypedIntArraySerializer extends TypedSerializer[Array[Int]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      IntArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      IntArraySerializer.read(inputStream)
    }

    def name: String = "array[int]"
  }

  implicit object TypedLongArraySerializer extends TypedSerializer[Array[Long]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      LongArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      LongArraySerializer.read(inputStream)
    }

    def name: String = "array[long]"
  }

  implicit object TypedDoubleArraySerializer extends TypedSerializer[Array[Double]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      DoubleArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      DoubleArraySerializer.read(inputStream)
    }

    def name: String = "array[double]"
  }

  implicit object TypedShortArraySerializer extends TypedSerializer[Array[Short]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      ShortArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      ShortArraySerializer.read(inputStream)
    }

    def name: String = "array[short]"
  }

  implicit object TypedFloatArraySerializer extends TypedSerializer[Array[Float]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      FloatArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      FloatArraySerializer.read(inputStream)
    }

    def name: String = "array[float]"
  }

  implicit object TypedByteArraySerializer extends TypedSerializer[Array[Byte]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      ByteArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      ByteArraySerializer.read(inputStream)
    }

    def name: String = "array[byte]"
  }
}
