package com.stefansavev.core.serialization.core

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import com.stefansavev.randomprojections.utils.Utils

object IntSerializer{
  val bytes = Array.ofDim[Byte](4)
  def toByteArray(value: Int): Array[Byte] = {
    ByteBuffer.wrap(bytes).putInt(value)
    bytes
  }

  def toInt(bytes:  Array[Byte]): Int = {
    return ByteBuffer.wrap(bytes).getInt()
  }

  def write(outputStream: OutputStream, value: Int): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Int = {
    inputStream.read(bytes)
    toInt(bytes)
  }

  def sizeInBytes: Long = {
    2
  }
}

object ShortSerializer{
  val bytes = Array.ofDim[Byte](2)

  def toByteArray(value: Short): Array[Byte] = {
    ByteBuffer.wrap(bytes).putShort(value)
    bytes
  }

  def toShort(bytes:  Array[Byte]): Short = {
    return ByteBuffer.wrap(bytes).getShort()
  }

  def write(outputStream: OutputStream, value: Short): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Short = {
    inputStream.read(bytes)
    toShort(bytes)
  }

  def sizeInBytes: Long = {
    2
  }
}

object FloatSerializer{
  val bytes = Array.ofDim[Byte](4)

  def toByteArray(value: Float): Array[Byte] = {
    ByteBuffer.wrap(bytes).putFloat(value)
    bytes
  }

  def toShort(bytes:  Array[Byte]): Float = {
    return ByteBuffer.wrap(bytes).getFloat()
  }

  def write(outputStream: OutputStream, value: Float): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Float = {
    inputStream.read(bytes)
    toShort(bytes)
  }

  def sizeInBytes: Long = {
    4
  }
}

object StringSerializer{

  def write(outputStream: OutputStream, value: String): Unit = {
    IntSerializer.write(outputStream, value.length)
    val buffer = ByteBuffer.allocate(value.length*2)
    var i = 0
    while(i < value.length){
      buffer.putChar(value(i))
      i += 1
    }
    buffer.rewind()
    outputStream.write(buffer.array())
  }

  def read(inputStream: InputStream): String = {
    val len = IntSerializer.read(inputStream)
    val buffer = Array.ofDim[Byte](len*2)
    inputStream.read(buffer)
    val byteBuffer = ByteBuffer.wrap(buffer)
    val output = Array.ofDim[Char](len)
    var i = 0
    while(i < len){
      output(i) = byteBuffer.getChar()
      i += 1
    }
    new String(output)
  }

  def sizeInBytes(input: String): Long = {
    4 + 2*input.length
  }
}

object DoubleSerializer{
  val bytes = Array.ofDim[Byte](8)

  def toByteArray(value: Double): Array[Byte] = {
    ByteBuffer.wrap(bytes).putDouble(value)
    bytes
  }

  def toDouble(bytes:  Array[Byte]): Double = {
    return ByteBuffer.wrap(bytes).getDouble()
  }

  def write(outputStream: OutputStream, value: Double): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Double = {
    inputStream.read(bytes)
    toDouble(bytes)
  }

  def sizeInBytes: Long = {
    8
  }
}

object LongSerializer{
  val bytes = Array.ofDim[Byte](8)

  def toByteArray(value: Long): Array[Byte] = {
    ByteBuffer.wrap(bytes).putLong(value)
    bytes
  }

  def toLong(bytes:  Array[Byte]): Long = {
    return ByteBuffer.wrap(bytes).getLong()
  }

  def write(outputStream: OutputStream, value: Long): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Long = {
    inputStream.read(bytes)
    toLong(bytes)
  }

  def sizeInBytes: Long = {
    4
  }
}

object DoubleArraySerializer{

  def writeToBufferWithoutArrayLength(values: Array[Double], output: Array[Byte]): Unit = {
    val buff = ByteBuffer.wrap(output)
    var i = 0
    while(i < values.length){
      buff.putDouble(values(i))
      i += 1
    }
  }

  def write(outputStream: OutputStream, values: Array[Double]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      DoubleSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Double] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Double](len)
    var i = 0
    while(i < len){
      values(i) = DoubleSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Double]): Long = {
    IntSerializer.sizeInBytes + DoubleSerializer.sizeInBytes*input.length
  }
}

object IntArraySerializer{
  def write(outputStream: OutputStream, values: Array[Int]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      IntSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Int] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Int](len)
    var i = 0
    while(i < len){
      values(i) = IntSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Int]): Long = {
    IntSerializer.sizeInBytes + IntSerializer.sizeInBytes*input.length
  }
}


object FloatArraySerializer{
  def write(outputStream: OutputStream, values: Array[Float]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      FloatSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Float] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Float](len)
    var i = 0
    while(i < len){
      values(i) = FloatSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Float]): Long = {
    IntSerializer.sizeInBytes + FloatSerializer.sizeInBytes*input.length
  }
}

object ByteArraySerializer{
  def write(outputStream: OutputStream, values: Array[Byte]): Unit = {
    IntSerializer.write(outputStream, values.length)
    outputStream.write(values)
  }

  def read(inputStream: InputStream): Array[Byte] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Byte](len)
    inputStream.read(values)
    values
  }

  def sizeInBytes(input: Array[Byte]): Long = {
    IntSerializer.sizeInBytes + input.length
  }
}

object ShortArraySerializer{
  def write(outputStream: OutputStream, values: Array[Short]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      ShortSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Short] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Short](len)
    var i = 0
    while(i < len){
      values(i) = ShortSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Short]): Long = {
    IntSerializer.sizeInBytes + ShortSerializer.sizeInBytes*input.length
  }
}

class AsyncLongArraySerializer(output: Array[Long], offset: Int){
  //output = request how much more
  var state = 0
  var remainingElements = 0
  var outputPos: Int = offset

  def readNextItems(buffer: ByteBuffer, n: Int): Unit = {
    var i = 0
    var j = outputPos
    val output = this.output
    while(i < n){
      output(j) = buffer.getLong
      i += 1
      j += 1
    }
    outputPos = j
    remainingElements -= n
  }

  def process(bytes: Array[Byte], fromPos: Int, toPos: Int): Int = {
    val buffer = ByteBuffer.wrap(bytes)
    buffer.position(fromPos)
    if (state == 0){
      remainingElements = buffer.getInt
      if (remainingElements < 0){
        Utils.internalError()
      }
      state = 1
      val available = Math.min((toPos - fromPos - 4)/8 , remainingElements)
      readNextItems(buffer, available)
      8*(remainingElements - available)
    }
    else{
      val available = Math.min((toPos - fromPos)/8, remainingElements)
      readNextItems(buffer, available)
      8*(remainingElements - available)
    }
  }

  def getArray(): Array[Long] = output

}

object LongArraySerializer{
  def write(outputStream: OutputStream, values: Array[Long]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      LongSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Long] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Long](len)
    var i = 0
    while(i < len){
      values(i) = LongSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Long]): Long = {
    IntSerializer.sizeInBytes + LongSerializer.sizeInBytes*input.length
  }
}

object ImplicitSerializers{
  implicit class IntSerializerExt(outputStream: OutputStream) {
    def writeInt(value: Int): Unit = {
      IntSerializer.write(outputStream, value)
    }
  }

  implicit class IntDeSerializerExt(inputStream: InputStream) {
    def readInt(): Int = {
      IntSerializer.read(inputStream)
    }
  }

  implicit class DoubleArraySerializerExt(outputStream: OutputStream) {
    def writeDoubleArray(values: Array[Double]): Unit = {
      DoubleArraySerializer.write(outputStream, values)
    }
  }

  implicit class DoubleArrayDeSerializerExt(inputStream: InputStream) {
    def readDoubleArray(): Array[Double] = {
      DoubleArraySerializer.read(inputStream)
    }
  }

}
