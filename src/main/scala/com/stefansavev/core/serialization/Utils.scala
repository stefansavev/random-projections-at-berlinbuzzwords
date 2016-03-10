package com.stefansavev.core.serialization

import java.io._

object Utils{

  def toFile[A](serializer: TypedSerializer[A], outputFile: String, input: A): Unit = {
    toFile(serializer, new File(outputFile), input)
  }

  def toFile[A](serializer: TypedSerializer[A], outputFile: File, input: A): Unit = {
    val outputStream = new BufferedOutputStream(new FileOutputStream(outputFile))
    serializer.toBinary(outputStream, input)
    outputStream.close()
  }

  def toBytes[A](serializer: TypedSerializer[A], input: A): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    serializer.toBinary(outputStream, input)
    val output = outputStream.toByteArray
    outputStream.close()
    output
  }

  def fromBytes[A](serializer: TypedSerializer[A], input: Array[Byte]): A = {
    val inputStream = new ByteArrayInputStream(input)
    val output = serializer.fromBinary(inputStream)
    inputStream.close()
    output
  }

  def fromFile[A](serializer: TypedSerializer[A], inputFile: File): A = {
    val inputStream = new BufferedInputStream(new FileInputStream(inputFile))
    val output = serializer.fromBinary(inputStream)
    inputStream.close()
    output
  }

  def fromFile[A](serializer: TypedSerializer[A], inputFile: String): A = {
    fromFile(serializer, new File(inputFile))
  }
}
