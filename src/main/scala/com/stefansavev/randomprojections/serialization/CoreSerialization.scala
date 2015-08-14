package com.stefansavev.randomprojections.serialization

import java.io._

//approach is losely based on "Scala for generic programmers"

trait TypedSerializer[T]{
  def toBinary(outputStream: OutputStream, input: T): Unit
  def fromBinary(inputStream: InputStream): T
}

object Core{
  /* moved to TupleSerializers via code autogen, left here as an example only
  class Tuple2Serializer[A, B](serA: TypedSerializer[A], serB: TypedSerializer[B]) extends TypedSerializer[(A, B)] {
    def toBinary(outputStream: OutputStream, input: (A, B)): Unit = {
      serA.toBinary(outputStream, input._1)
      serB.toBinary(outputStream, input._2)
    }

    def fromBinary(inputStream: InputStream): (A, B) = {
      val a = serA.fromBinary(inputStream)
      val b = serB.fromBinary(inputStream)
      (a, b)
    }
  }
  implicit def tuple2Serializer[A, B](implicit serA: TypedSerializer[A], serB: TypedSerializer[B]): Tuple2Serializer[A, B] = {
    new Tuple2Serializer[A, B](serA, serB)
  }
  */

  //Iso means isomorphism, just mapping from A to B, and back
  trait Iso[A, B]{
    type Output = B

    def from(input: A): B
    def to(output: B): A
  }

  def toFile[A](serializer: TypedSerializer[A], outputFile: String, input: A): Unit ={
    val outputStream = new BufferedOutputStream(new FileOutputStream(outputFile))
    serializer.toBinary(outputStream, input)
    outputStream.close()
  }

  def fromFile[A](serializer: TypedSerializer[A], inputFile: String): A = {
    val inputStream = new BufferedInputStream(new FileInputStream(inputFile))
    val output = serializer.fromBinary(inputStream)
    inputStream.close()
    output
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
