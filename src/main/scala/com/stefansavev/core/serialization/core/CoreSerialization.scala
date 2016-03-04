package com.stefansavev.core.serialization.core

import java.io._
import javax.imageio.stream.MemoryCacheImageOutputStream

import com.stefansavev.core.serialization.core.PrimitiveTypeSerializers.TypedIntSerializer

//approach is loosely based on "Scala for generic programmers"
trait TypedSerializer[T]{
  type SerializerType = T
  def toBinary(outputStream: OutputStream, input: T): Unit
  def fromBinary(inputStream: InputStream): T
}

object Core{
  abstract class TypeTag[A](implicit mf: Manifest[A]){
    def tag: Int
    def manifest: Manifest[A] = mf
  }

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

  class Subtype1Serializer[BaseType, SubType1 <: BaseType](tag1: TypeTag[SubType1], subTypeSer1 : TypedSerializer[SubType1]) extends TypedSerializer[BaseType] {

    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else{
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else{
          throw new IllegalStateException("Unknown tag in deserialization")
      }
    }
  }

  class Subtype2Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType](
      tag1: TypeTag[SubType1],
      tag2: TypeTag[SubType2],
      subTypeSer1 : TypedSerializer[SubType1],
      subTypeSer2 : TypedSerializer[SubType2]) extends TypedSerializer[BaseType] {

    if (tag1.tag == tag2.tag){
      throw new IllegalStateException("Subtypes should have different tags")
    }
    if (tag1.manifest.runtimeClass.equals(tag2.manifest.runtimeClass)){
      throw new IllegalStateException("Subtypes should be of different classes")
    }
    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else if (tag2.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag2.tag)
        subTypeSer2.toBinary(outputStream, input.asInstanceOf[SubType2])
      }
      else{
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else if (inputTag == tag2.tag){
        subTypeSer2.fromBinary(inputStream)
      }
      else{
        throw new IllegalStateException("Unknown tag in deserialization")
      }
    }
  }

  class Subtype3Serializer[BaseType,
                           SubType1 <: BaseType,
                           SubType2 <: BaseType,
                           SubType3 <: BaseType](
                                tag1: TypeTag[SubType1],
                                tag2: TypeTag[SubType2],
                                tag3: TypeTag[SubType3],
                                subTypeSer1 : TypedSerializer[SubType1],
                                subTypeSer2 : TypedSerializer[SubType2],
                                subTypeSer3 : TypedSerializer[SubType3]) extends TypedSerializer[BaseType] {

    if (tag1.tag == tag2.tag || tag1.tag == tag3.tag || tag2.tag == tag3.tag){
      throw new IllegalStateException("Subtypes should have different tags")
    }

    if (tag1.manifest.runtimeClass.equals(tag2.manifest.runtimeClass)){
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    if (tag1.manifest.runtimeClass.equals(tag3.manifest.runtimeClass)){
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    if (tag2.manifest.runtimeClass.equals(tag3.manifest.runtimeClass)){
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else if (tag2.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag2.tag)
        subTypeSer2.toBinary(outputStream, input.asInstanceOf[SubType2])
      }
      else if (tag3.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag3.tag)
        subTypeSer3.toBinary(outputStream, input.asInstanceOf[SubType3])
      }
      else{
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else if (inputTag == tag2.tag){
        subTypeSer2.fromBinary(inputStream)
      }
      else if (inputTag == tag3.tag){
        subTypeSer3.fromBinary(inputStream)
      }
      else{
        throw new IllegalStateException("Unknown tag in deserialization")
      }
    }
  }

  class Subtype4Serializer[BaseType,
                            SubType1 <: BaseType,
                            SubType2 <: BaseType,
                            SubType3 <: BaseType,
                            SubType4 <: BaseType](
                         tag1: TypeTag[SubType1],
                         tag2: TypeTag[SubType2],
                         tag3: TypeTag[SubType3],
                         tag4: TypeTag[SubType4],
                         subTypeSer1 : TypedSerializer[SubType1],
                         subTypeSer2 : TypedSerializer[SubType2],
                         subTypeSer3 : TypedSerializer[SubType3],
                         subTypeSer4 : TypedSerializer[SubType4]) extends TypedSerializer[BaseType] {

    val allTags = Array(tag1, tag2, tag3, tag4)
    if (allTags.map(_.tag).distinct.length != allTags.length){
      throw new IllegalStateException("Subtypes should have different tags")
    }

    if (allTags.map(_.manifest.runtimeClass).distinct.length != allTags.length){
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else if (tag2.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag2.tag)
        subTypeSer2.toBinary(outputStream, input.asInstanceOf[SubType2])
      }
      else if (tag3.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag3.tag)
        subTypeSer3.toBinary(outputStream, input.asInstanceOf[SubType3])
      }
      else if (tag4.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag4.tag)
        subTypeSer4.toBinary(outputStream, input.asInstanceOf[SubType4])
      }
      else{
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else if (inputTag == tag2.tag){
        subTypeSer2.fromBinary(inputStream)
      }
      else if (inputTag == tag3.tag){
        subTypeSer3.fromBinary(inputStream)
      }
      else if (inputTag == tag4.tag){
        subTypeSer4.fromBinary(inputStream)
      }
      else{
        throw new IllegalStateException("Unknown tag in deserialization")
      }
    }
  }


  class Subtype5Serializer[BaseType,
  SubType1 <: BaseType,
  SubType2 <: BaseType,
  SubType3 <: BaseType,
  SubType4 <: BaseType,
  SubType5 <: BaseType](
                         tag1: TypeTag[SubType1],
                         tag2: TypeTag[SubType2],
                         tag3: TypeTag[SubType3],
                         tag4: TypeTag[SubType4],
                         tag5: TypeTag[SubType5],
                         subTypeSer1 : TypedSerializer[SubType1],
                         subTypeSer2 : TypedSerializer[SubType2],
                         subTypeSer3 : TypedSerializer[SubType3],
                         subTypeSer4 : TypedSerializer[SubType4],
                         subTypeSer5 : TypedSerializer[SubType5]) extends TypedSerializer[BaseType] {

    val allTags = Array(tag1, tag2, tag3, tag4, tag5)
    if (allTags.map(_.tag).distinct.length != allTags.length){
      throw new IllegalStateException("Subtypes should have different tags")
    }

    if (allTags.map(_.manifest.runtimeClass).distinct.length != allTags.length){
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else if (tag2.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag2.tag)
        subTypeSer2.toBinary(outputStream, input.asInstanceOf[SubType2])
      }
      else if (tag3.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag3.tag)
        subTypeSer3.toBinary(outputStream, input.asInstanceOf[SubType3])
      }
      else if (tag4.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag4.tag)
        subTypeSer4.toBinary(outputStream, input.asInstanceOf[SubType4])
      }
      else if (tag5.manifest.runtimeClass.equals(input.getClass)){
        TypedIntSerializer.toBinary(outputStream, tag5.tag)
        subTypeSer5.toBinary(outputStream, input.asInstanceOf[SubType5])
      }
      else{
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else if (inputTag == tag2.tag){
        subTypeSer2.fromBinary(inputStream)
      }
      else if (inputTag == tag3.tag){
        subTypeSer3.fromBinary(inputStream)
      }
      else if (inputTag == tag4.tag){
        subTypeSer4.fromBinary(inputStream)
      }
      else if (inputTag == tag5.tag){
        subTypeSer5.fromBinary(inputStream)
      }
      else{
        throw new IllegalStateException("Unknown tag in deserialization")
      }
    }
  }

  implicit def subtype1Serializer[BaseType, SubType1 <: BaseType](implicit typeTag1: TypeTag[SubType1], subTypeSer1 : TypedSerializer[SubType1]): Subtype1Serializer[BaseType, SubType1] = {
    new Subtype1Serializer[BaseType, SubType1](typeTag1, subTypeSer1)
  }

  implicit def subtype2Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType](
       implicit
       typeTag1: TypeTag[SubType1],
       typeTag2: TypeTag[SubType2],
       subTypeSer1 : TypedSerializer[SubType1],
       subTypeSer2 : TypedSerializer[SubType2]): Subtype2Serializer[BaseType, SubType1, SubType2] = {
    new Subtype2Serializer[BaseType, SubType1, SubType2](typeTag1, typeTag2, subTypeSer1, subTypeSer2)
  }

  implicit def subtype3Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType, SubType3 <: BaseType](
                                                                                         implicit
                                                                                         typeTag1: TypeTag[SubType1],
                                                                                         typeTag2: TypeTag[SubType2],
                                                                                         typeTag3: TypeTag[SubType3],
                                                                                         subTypeSer1 : TypedSerializer[SubType1],
                                                                                         subTypeSer2 : TypedSerializer[SubType2],
                                                                                         subTypeSer3 : TypedSerializer[SubType3]): Subtype3Serializer[BaseType, SubType1, SubType2, SubType3] = {
    new Subtype3Serializer[BaseType, SubType1, SubType2, SubType3](typeTag1, typeTag2, typeTag3, subTypeSer1, subTypeSer2, subTypeSer3)
  }

  implicit def subtype4Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType, SubType3 <: BaseType, SubType4 <: BaseType](
                                                                                                               implicit
                                                                                                               typeTag1: TypeTag[SubType1],
                                                                                                               typeTag2: TypeTag[SubType2],
                                                                                                               typeTag3: TypeTag[SubType3],
                                                                                                               typeTag4: TypeTag[SubType4],
                                                                                                               subTypeSer1 : TypedSerializer[SubType1],
                                                                                                               subTypeSer2 : TypedSerializer[SubType2],
                                                                                                               subTypeSer3 : TypedSerializer[SubType3],
                                                                                                               subTypeSer4 : TypedSerializer[SubType4]): Subtype4Serializer[BaseType, SubType1, SubType2, SubType3, SubType4] = {
    new Subtype4Serializer[BaseType, SubType1, SubType2, SubType3, SubType4](typeTag1, typeTag2, typeTag3, typeTag4, subTypeSer1, subTypeSer2, subTypeSer3, subTypeSer4)
  }

  implicit def subtype5Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType, SubType3 <: BaseType, SubType4 <: BaseType, SubType5 <: BaseType](
                                                                                                                                     implicit
                                                                                                                                     typeTag1: TypeTag[SubType1],
                                                                                                                                     typeTag2: TypeTag[SubType2],
                                                                                                                                     typeTag3: TypeTag[SubType3],
                                                                                                                                     typeTag4: TypeTag[SubType4],
                                                                                                                                     typeTag5: TypeTag[SubType5],
                                                                                                                                     subTypeSer1 : TypedSerializer[SubType1],
                                                                                                                                     subTypeSer2 : TypedSerializer[SubType2],
                                                                                                                                     subTypeSer3 : TypedSerializer[SubType3],
                                                                                                                                     subTypeSer4 : TypedSerializer[SubType4],
                                                                                                                                     subTypeSer5 : TypedSerializer[SubType5]): Subtype5Serializer[BaseType, SubType1, SubType2, SubType3, SubType4, SubType5] = {
    new Subtype5Serializer[BaseType, SubType1, SubType2, SubType3, SubType4, SubType5](typeTag1, typeTag2, typeTag3, typeTag4, typeTag5, subTypeSer1, subTypeSer2, subTypeSer3, subTypeSer4, subTypeSer5)
  }
  //Iso means isomorphism, just mapping from A to B, and back
  trait Iso[A, B]{
    type Input = A
    type Output = B

    def from(input: A): B
    def to(output: B): A
  }

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
