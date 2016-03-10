package com.stefansavev.core.serialization

import java.io.{InputStream, OutputStream}

import com.stefansavev.core.serialization.PrimitiveTypeSerializers.TypedIntSerializer

import scala.reflect.ClassTag

object GenericArraySerializer {

  /**
   * A serializer for handling generic arrays. Primitive arrays are handled by PrimitiveTypeSerializers object
   * Here I want to handle AnyRef (non-primitive types) and supply a class tag
   * @param ev
   * @param nestedSerializer
   * @tparam T
   */
  //
  class GenericArraySerializer[T: ClassTag](ev: T <:< AnyRef, nestedSerializer: TypedSerializer[T]) extends TypedSerializer[Array[T]] {
    val clazz = scala.reflect.classTag[T].runtimeClass
    if (clazz.equals(Int.getClass) || clazz.equals(Double.getClass)) {
      throw new IllegalStateException("GenericArraySerializer should not be applied to primitive types")
    }

    def toBinary(outputStream: OutputStream, input: Array[T]): Unit = {
      TypedIntSerializer.toBinary(outputStream, input.length)
      var i = 0
      while (i < input.length) {
        nestedSerializer.toBinary(outputStream, input(i))
        i += 1
      }
    }

    def fromBinary(inputStream: InputStream): Array[T] = {
      val len = TypedIntSerializer.fromBinary(inputStream)
      val output = Array.ofDim[T](len)
      var i = 0
      while (i < len) {
        output(i) = nestedSerializer.fromBinary(inputStream)
        i += 1
      }
      output
    }

    def name: String = s"GenericArraySerializer(${nestedSerializer.name})"
  }

  implicit def genericArraySerializer[T: ClassTag](implicit ev: T <:< AnyRef, nestedSerializer: TypedSerializer[T]): GenericArraySerializer[T] = {
    new GenericArraySerializer[T](ev, nestedSerializer)
  }
}
