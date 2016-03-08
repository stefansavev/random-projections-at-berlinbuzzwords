package com.stefansavev.core.serialization.core

import java.io._

import com.stefansavev.core.serialization.core.PrimitiveTypeSerializers.TypedIntSerializer

//approach is loosely based on "Scala for generic programmers"
trait TypedSerializer[T] {
  type SerializerType = T

  def toBinary(outputStream: OutputStream, input: T): Unit

  def fromBinary(inputStream: InputStream): T
}

trait Iso[A, B]{
  type Input = A
  type Output = B

  def from(input: A): B
  def to(output: B): A
}

object Core{
  //Iso means isomorphism, a mapping from A to B, and back
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

  class IsoSerializer[A, B](iso: Iso[A, B], serB: TypedSerializer[B]) extends TypedSerializer[A]{
    def toBinary(outputStream: OutputStream, input: A): Unit = {
      serB.toBinary(outputStream, iso.from(input))
    }

    def fromBinary(inputStream: InputStream): A = {
      iso.to(serB.fromBinary(inputStream))
    }
  }

  implicit def isoSerializer[A, B](implicit iso: Iso[A, B], serB: TypedSerializer[B]): IsoSerializer[A, B] = {
    new IsoSerializer[A, B](iso, serB)
  }

}
