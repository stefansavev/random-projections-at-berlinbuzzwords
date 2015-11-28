package com.stefansavev.randomprojections.serialization.core

import java.io.{InputStream, OutputStream}

import com.stefansavev.randomprojections.serialization._

object PrimitiveTypeSerializers {

  implicit object TypedIntSerializer extends TypedSerializer[Int] {
    def toBinary(outputStream: OutputStream, input: Int): Unit = {
      IntSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): Int = {
      IntSerializer.read(inputStream)
    }

    def sizeInBytes(memoryTracker: MemoryTracker, input: Int): Long = {
      MemoryTrackingUtils.withNestingInfo(memoryTracker, input, 4)
    }
  }

  implicit object TypedStringSerializer extends TypedSerializer[String] {
    def toBinary(outputStream: OutputStream, input: String): Unit = {
      StringSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): String = {
      StringSerializer.read(inputStream)
    }

    def sizeInBytes(memoryTracker: MemoryTracker, input: String): Long = {
      StringSerializer.sizeInBytes(input)
    }
  }

  implicit object TypedDoubleSerializer extends TypedSerializer[Double] {
    def toBinary(outputStream: OutputStream, input: Double): Unit = {
      DoubleSerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): Double = {
      DoubleSerializer.read(inputStream)
    }

    def sizeInBytes(memoryTracker: MemoryTracker, input: Double): Long = {
      DoubleSerializer.sizeInBytes
    }
  }

  implicit object TypedIntArraySerializer extends TypedSerializer[Array[Int]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      IntArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      IntArraySerializer.read(inputStream)
    }

    def sizeInBytes(memoryTracker: MemoryTracker, input: SerializerType): Long = {
      MemoryTrackingUtils.withNestingInfo(memoryTracker, input, IntArraySerializer.sizeInBytes(input))
    }
  }

  implicit object TypedDoubleArraySerializer extends TypedSerializer[Array[Double]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      DoubleArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      DoubleArraySerializer.read(inputStream)
    }

    def sizeInBytes(memoryTracker: MemoryTracker, input: SerializerType): Long = {
      MemoryTrackingUtils.withNestingInfo(memoryTracker, input, DoubleArraySerializer.sizeInBytes(input))
    }
  }

  implicit object TypedShortArraySerializer extends TypedSerializer[Array[Short]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      ShortArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      ShortArraySerializer.read(inputStream)
    }

    def sizeInBytes(memoryTracker: MemoryTracker, input: SerializerType): Long = {
      ShortArraySerializer.sizeInBytes(input)
    }
  }

  implicit object TypedFloatArraySerializer extends TypedSerializer[Array[Float]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      FloatArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      FloatArraySerializer.read(inputStream)
    }

    def sizeInBytes(memoryTracker: MemoryTracker, input: SerializerType): Long = {
      FloatArraySerializer.sizeInBytes(input)
    }
  }

  implicit object TypedByteArraySerializer extends TypedSerializer[Array[Byte]]{
    def toBinary(outputStream: OutputStream, input: SerializerType): Unit = {
      ByteArraySerializer.write(outputStream, input)
    }

    def fromBinary(inputStream: InputStream): SerializerType = {
      ByteArraySerializer.read(inputStream)
    }

    def sizeInBytes(memoryTracker: MemoryTracker, input: SerializerType): Long = {
      ByteArraySerializer.sizeInBytes(input)
    }
  }
}
