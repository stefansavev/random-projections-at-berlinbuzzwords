package com.stefansavev.core.serialization

import java.io.{InputStream, OutputStream}

import com.stefansavev.core.serialization.PrimitiveTypeSerializers.TypedIntSerializer

object SubtypeSerializers {

  abstract class TypeTag[A](implicit mf: Manifest[A]) {
    def tag: Int

    def manifest: Manifest[A] = mf
  }

  class Subtype1Serializer[BaseType, SubType1 <: BaseType](tag1: TypeTag[SubType1], subTypeSer1: TypedSerializer[SubType1]) extends TypedSerializer[BaseType] {

    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else {
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else {
        throw new IllegalStateException("Unknown tag in deserialization")
      }
    }

    def name: String = s"subtype1(${subTypeSer1.name})"
  }

  class Subtype2Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType](
                                                                                  tag1: TypeTag[SubType1],
                                                                                  tag2: TypeTag[SubType2],
                                                                                  subTypeSer1: TypedSerializer[SubType1],
                                                                                  subTypeSer2: TypedSerializer[SubType2]) extends TypedSerializer[BaseType] {

    if (tag1.tag == tag2.tag) {
      throw new IllegalStateException("Subtypes should have different tags")
    }
    if (tag1.manifest.runtimeClass.equals(tag2.manifest.runtimeClass)) {
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else if (tag2.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag2.tag)
        subTypeSer2.toBinary(outputStream, input.asInstanceOf[SubType2])
      }
      else {
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else if (inputTag == tag2.tag) {
        subTypeSer2.fromBinary(inputStream)
      }
      else {
        throw new IllegalStateException("Unknown tag in deserialization")
      }
    }

    def name: String = s"subtype2(${subTypeSer1.name}, ${subTypeSer2.name})"
  }

  class Subtype3Serializer[BaseType,
  SubType1 <: BaseType,
  SubType2 <: BaseType,
  SubType3 <: BaseType](
                         tag1: TypeTag[SubType1],
                         tag2: TypeTag[SubType2],
                         tag3: TypeTag[SubType3],
                         subTypeSer1: TypedSerializer[SubType1],
                         subTypeSer2: TypedSerializer[SubType2],
                         subTypeSer3: TypedSerializer[SubType3]) extends TypedSerializer[BaseType] {

    if (tag1.tag == tag2.tag || tag1.tag == tag3.tag || tag2.tag == tag3.tag) {
      throw new IllegalStateException("Subtypes should have different tags")
    }

    if (tag1.manifest.runtimeClass.equals(tag2.manifest.runtimeClass)) {
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    if (tag1.manifest.runtimeClass.equals(tag3.manifest.runtimeClass)) {
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    if (tag2.manifest.runtimeClass.equals(tag3.manifest.runtimeClass)) {
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else if (tag2.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag2.tag)
        subTypeSer2.toBinary(outputStream, input.asInstanceOf[SubType2])
      }
      else if (tag3.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag3.tag)
        subTypeSer3.toBinary(outputStream, input.asInstanceOf[SubType3])
      }
      else {
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else if (inputTag == tag2.tag) {
        subTypeSer2.fromBinary(inputStream)
      }
      else if (inputTag == tag3.tag) {
        subTypeSer3.fromBinary(inputStream)
      }
      else {
        throw new IllegalStateException("Unknown tag in deserialization")
      }
    }

    def name: String = s"subtype3(${subTypeSer1.name}, ${subTypeSer2.name}, ${subTypeSer3.name})"
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
                         subTypeSer1: TypedSerializer[SubType1],
                         subTypeSer2: TypedSerializer[SubType2],
                         subTypeSer3: TypedSerializer[SubType3],
                         subTypeSer4: TypedSerializer[SubType4]) extends TypedSerializer[BaseType] {

    val allTags = Array(tag1, tag2, tag3, tag4)
    if (allTags.map(_.tag).distinct.length != allTags.length) {
      throw new IllegalStateException("Subtypes should have different tags")
    }

    if (allTags.map(_.manifest.runtimeClass).distinct.length != allTags.length) {
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else if (tag2.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag2.tag)
        subTypeSer2.toBinary(outputStream, input.asInstanceOf[SubType2])
      }
      else if (tag3.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag3.tag)
        subTypeSer3.toBinary(outputStream, input.asInstanceOf[SubType3])
      }
      else if (tag4.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag4.tag)
        subTypeSer4.toBinary(outputStream, input.asInstanceOf[SubType4])
      }
      else {
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else if (inputTag == tag2.tag) {
        subTypeSer2.fromBinary(inputStream)
      }
      else if (inputTag == tag3.tag) {
        subTypeSer3.fromBinary(inputStream)
      }
      else if (inputTag == tag4.tag) {
        subTypeSer4.fromBinary(inputStream)
      }
      else {
        throw new IllegalStateException("Unknown tag in deserialization")
      }
    }

    def name: String = s"subtype4(${subTypeSer1.name}, ${subTypeSer2.name}, ${subTypeSer3.name}, ${subTypeSer4.name})"
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
                         subTypeSer1: TypedSerializer[SubType1],
                         subTypeSer2: TypedSerializer[SubType2],
                         subTypeSer3: TypedSerializer[SubType3],
                         subTypeSer4: TypedSerializer[SubType4],
                         subTypeSer5: TypedSerializer[SubType5]) extends TypedSerializer[BaseType] {

    val allTags = Array(tag1, tag2, tag3, tag4, tag5)
    if (allTags.map(_.tag).distinct.length != allTags.length) {
      throw new IllegalStateException("Subtypes should have different tags")
    }

    if (allTags.map(_.manifest.runtimeClass).distinct.length != allTags.length) {
      throw new IllegalStateException("Subtypes should be of different classes")
    }

    def toBinary(outputStream: OutputStream, input: BaseType): Unit = {
      if (tag1.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag1.tag)
        subTypeSer1.toBinary(outputStream, input.asInstanceOf[SubType1])
      }
      else if (tag2.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag2.tag)
        subTypeSer2.toBinary(outputStream, input.asInstanceOf[SubType2])
      }
      else if (tag3.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag3.tag)
        subTypeSer3.toBinary(outputStream, input.asInstanceOf[SubType3])
      }
      else if (tag4.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag4.tag)
        subTypeSer4.toBinary(outputStream, input.asInstanceOf[SubType4])
      }
      else if (tag5.manifest.runtimeClass.equals(input.getClass)) {
        TypedIntSerializer.toBinary(outputStream, tag5.tag)
        subTypeSer5.toBinary(outputStream, input.asInstanceOf[SubType5])
      }
      else {
        throw new IllegalStateException("Unsupported subtype in serialization")
      }
    }

    def fromBinary(inputStream: InputStream): BaseType = {
      val inputTag = TypedIntSerializer.fromBinary(inputStream)
      if (inputTag == tag1.tag) {
        subTypeSer1.fromBinary(inputStream)
      }
      else if (inputTag == tag2.tag) {
        subTypeSer2.fromBinary(inputStream)
      }
      else if (inputTag == tag3.tag) {
        subTypeSer3.fromBinary(inputStream)
      }
      else if (inputTag == tag4.tag) {
        subTypeSer4.fromBinary(inputStream)
      }
      else if (inputTag == tag5.tag) {
        subTypeSer5.fromBinary(inputStream)
      }
      else {
        throw new IllegalStateException("Unknown tag in deserialization")
      }
    }

    def name: String = s"subtype5(${subTypeSer1.name}, ${subTypeSer2.name}, ${subTypeSer3.name}, ${subTypeSer4.name}, ${subTypeSer5.name})"
  }

  implicit def subtype1Serializer[BaseType, SubType1 <: BaseType](implicit typeTag1: TypeTag[SubType1], subTypeSer1: TypedSerializer[SubType1]): Subtype1Serializer[BaseType, SubType1] = {
    new Subtype1Serializer[BaseType, SubType1](typeTag1, subTypeSer1)
  }

  implicit def subtype2Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType](
                                                                                         implicit
                                                                                         typeTag1: TypeTag[SubType1],
                                                                                         typeTag2: TypeTag[SubType2],
                                                                                         subTypeSer1: TypedSerializer[SubType1],
                                                                                         subTypeSer2: TypedSerializer[SubType2]): Subtype2Serializer[BaseType, SubType1, SubType2] = {
    new Subtype2Serializer[BaseType, SubType1, SubType2](typeTag1, typeTag2, subTypeSer1, subTypeSer2)
  }

  implicit def subtype3Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType, SubType3 <: BaseType](
                                                                                                               implicit
                                                                                                               typeTag1: TypeTag[SubType1],
                                                                                                               typeTag2: TypeTag[SubType2],
                                                                                                               typeTag3: TypeTag[SubType3],
                                                                                                               subTypeSer1: TypedSerializer[SubType1],
                                                                                                               subTypeSer2: TypedSerializer[SubType2],
                                                                                                               subTypeSer3: TypedSerializer[SubType3]): Subtype3Serializer[BaseType, SubType1, SubType2, SubType3] = {
    new Subtype3Serializer[BaseType, SubType1, SubType2, SubType3](typeTag1, typeTag2, typeTag3, subTypeSer1, subTypeSer2, subTypeSer3)
  }

  implicit def subtype4Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType, SubType3 <: BaseType, SubType4 <: BaseType](
                                                                                                                                     implicit
                                                                                                                                     typeTag1: TypeTag[SubType1],
                                                                                                                                     typeTag2: TypeTag[SubType2],
                                                                                                                                     typeTag3: TypeTag[SubType3],
                                                                                                                                     typeTag4: TypeTag[SubType4],
                                                                                                                                     subTypeSer1: TypedSerializer[SubType1],
                                                                                                                                     subTypeSer2: TypedSerializer[SubType2],
                                                                                                                                     subTypeSer3: TypedSerializer[SubType3],
                                                                                                                                     subTypeSer4: TypedSerializer[SubType4]): Subtype4Serializer[BaseType, SubType1, SubType2, SubType3, SubType4] = {
    new Subtype4Serializer[BaseType, SubType1, SubType2, SubType3, SubType4](typeTag1, typeTag2, typeTag3, typeTag4, subTypeSer1, subTypeSer2, subTypeSer3, subTypeSer4)
  }

  implicit def subtype5Serializer[BaseType, SubType1 <: BaseType, SubType2 <: BaseType, SubType3 <: BaseType, SubType4 <: BaseType, SubType5 <: BaseType](
                                                                                                                                                           implicit
                                                                                                                                                           typeTag1: TypeTag[SubType1],
                                                                                                                                                           typeTag2: TypeTag[SubType2],
                                                                                                                                                           typeTag3: TypeTag[SubType3],
                                                                                                                                                           typeTag4: TypeTag[SubType4],
                                                                                                                                                           typeTag5: TypeTag[SubType5],
                                                                                                                                                           subTypeSer1: TypedSerializer[SubType1],
                                                                                                                                                           subTypeSer2: TypedSerializer[SubType2],
                                                                                                                                                           subTypeSer3: TypedSerializer[SubType3],
                                                                                                                                                           subTypeSer4: TypedSerializer[SubType4],
                                                                                                                                                           subTypeSer5: TypedSerializer[SubType5]): Subtype5Serializer[BaseType, SubType1, SubType2, SubType3, SubType4, SubType5] = {
    new Subtype5Serializer[BaseType, SubType1, SubType2, SubType3, SubType4, SubType5](typeTag1, typeTag2, typeTag3, typeTag4, typeTag5, subTypeSer1, subTypeSer2, subTypeSer3, subTypeSer4, subTypeSer5)
  }

}
