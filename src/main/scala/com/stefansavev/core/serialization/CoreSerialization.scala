package com.stefansavev.core.serialization

import java.io._

import scala.reflect.ClassTag

/**
 * A serializer for type @tparam T
 *
 * approach is loosely based on "Scala for generic programmers"
 * http://www.cs.ox.ac.uk/jeremy.gibbons/publications/scalagp-jfp.pdf
 */
trait TypedSerializer[T] {
  type SerializerType = T

  def toBinary(outputStream: OutputStream, input: T): Unit

  def fromBinary(inputStream: InputStream): T

  def name: String
}

/**
 * Enrich the typed serializer with a name
 * Unfortunately because of the ClassTag it's not possible to use a trait
 * @tparam T
 */
abstract class NamedSerializer[T: ClassTag] extends TypedSerializer[T] {
  def name = scala.reflect.classTag[T].getClass.getName
}

/**
 * Iso means isomorphism, a mapping from A to B, and back
 * @tparam A input type e.g. Point(x: Int, y: Int)
 * @tparam B it could for example type Tuple2[Int, Int]
 */
trait Iso[A, B] {
  type Input = A
  type Output = B

  def from(input: A): B

  def to(output: B): A
}


