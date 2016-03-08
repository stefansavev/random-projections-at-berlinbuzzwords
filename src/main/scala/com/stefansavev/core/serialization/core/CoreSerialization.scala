package com.stefansavev.core.serialization.core

import java.io._

//approach is loosely based on "Scala for generic programmers"
trait TypedSerializer[T] {
  type SerializerType = T

  def toBinary(outputStream: OutputStream, input: T): Unit

  def fromBinary(inputStream: InputStream): T
}

//Iso means isomorphism, a mapping from A to B, and back
trait Iso[A, B]{
  type Input = A
  type Output = B

  def from(input: A): B
  def to(output: B): A
}


