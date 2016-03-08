package com.stefansavev.core.serialization

import java.io.File

import com.stefansavev.TemporaryFolderFixture
import com.stefansavev.core.serialization.core.Utils._
import com.stefansavev.core.serialization.core.PrimitiveTypeSerializers._
import com.stefansavev.core.serialization.core.SubtypeSerializers._
import com.stefansavev.core.serialization.core.TupleSerializers._
import com.stefansavev.core.serialization.core.IsoSerializers._
import com.stefansavev.core.serialization.core.{Iso, TypedSerializer}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

object SerializersTestUtils {
  def roundTripInputOutput[A](ser: TypedSerializer[A], input: A, file: File): A = {
    toFile(ser, file, input)
    val output = fromFile(ser, file)
    output
  }

  case class PointXY(x: Int, y: Double)

  case class BasePoint(x: Int, y: Double)

  class ExtendedPoint(val z: Int, x: Int, y: Double) extends BasePoint(x, y) {
    override def toString = "ExtendedPoint" +(z, x, y)
  }

  class SuperPoint(val k: Int, x: Int, y: Double) extends ExtendedPoint(-1, x, y){
    override def toString = "SuperPoint" + (z, x, y)
  }

  implicit object ExtendedPointTag extends TypeTag[ExtendedPoint]{
    def tag: Int = 2
  }

  implicit object SuperPointTag extends TypeTag[SuperPoint]{
    def tag: Int = 3
  }

  implicit object PointXYIso extends Iso[PointXY, (Int, Double)] {
    def from(input: PointXY): Output = (input.x, input.y)

    def to(input: Output): PointXY = {
      PointXY(input._1, input._2)
    }
  }

  implicit object ExtendedPointIso extends Iso[ExtendedPoint, (Int, Int, Double)] {
    def from(input: Input): Output = (input.z, input.x, input.y)

    def to(output: Output): Input = new ExtendedPoint(output._1, output._2, output._3)
  }

  implicit object SuperPointIso extends Iso[SuperPoint, (Int, Int, Double)] {
    def from(input: Input): Output = (input.k, input.x, input.y)

    def to(output: Output): Input = new SuperPoint(output._1, output._2, output._3)
  }

  //when the serializers are defined implicitly, they are
  implicit val basePointSer: TypedSerializer[BasePoint] = subtype2Serializer[BasePoint, ExtendedPoint, SuperPoint]

  //we don't need this because the isoSerializer implicitly picks PointXYIso
  //implicit val pointXYSerializer: TypedSerializer[PointXY] = isoSerializer[PointXY, (Int, Double)]

}

@RunWith(classOf[JUnitRunner])
class SerializersTest extends FunSuite with TemporaryFolderFixture with Matchers {
  import com.stefansavev.core.serialization.SerializersTestUtils._

  def serializerFromType[T](implicit e: TypedSerializer[T]) = implicitly[TypedSerializer[T]]

  def testSerialization[T](item: T, ser: TypedSerializer[T]): Unit = {
    val tmpFile = temporaryFolder.newFile()
    roundTripInputOutput(ser, item, tmpFile) should be(item)
  }

  test("serialize tuple2") {
    val items = (1, 2.0)
    val ser = tuple2Serializer[Int, Double]
    testSerialization(items, ser)
  }

  test("serialize tuple_of_tuple") {
    val items = (1, (2.0, 3))
    val ser = tuple2Serializer[Int, (Double, Int)]
    testSerialization(items, ser)
  }

  test("serialize PointXY") {
    val pointXY = PointXY(1, 3.4)
    val ser = serializerFromType[PointXY]
    testSerialization(pointXY, ser)
  }

  test("serialize tuple and point") {
    val items = ((1, 2), PointXY(1, 3.8))
    val ser = tuple2Serializer[(Int, Int), PointXY]
    testSerialization(items, ser)
  }

  test("serialize BasePoint") {
    val extPoint: BasePoint = new ExtendedPoint(1, 2, 3.0)
    val ser = serializerFromType[BasePoint]
    testSerialization(extPoint, ser)
  }

  test("serialize SuperPoint") {
    val superPoint: BasePoint = new SuperPoint(1, 2, 3.0)
    val ser = serializerFromType[BasePoint]
    testSerialization(superPoint, ser)
  }

  def testSerializationImplicitly[A](item: A)(implicit ser: TypedSerializer[A]): Unit = {
    testSerialization(item, ser)
  }

  test("implicit serializers"){
    val items = (1,2.0,(3,"4"))
    testSerialization(items, implicitly[TypedSerializer[(Int, Double,(Int, String))]])
    testSerializationImplicitly(items)
  }

  test("array of ints"){
    val arr = Array(1,2,3)
    testSerializationImplicitly(arr)
  }

  implicit class ToFileExtension[A](item: A){
    def toFile(outputFile: File)(implicit ser: TypedSerializer[A]): Unit = {
      import com.stefansavev.core.serialization.core.Utils.{toFile => toFileImpl}
      toFileImpl(ser, outputFile, item)
    }
  }

  implicit class ItemSerializerExt[T](ser: TypedSerializer[T]){
    def readItem(file: File): T = {
      fromFile(ser, file)
    }
  }

  test("serialization extensions"){
    val superPoint: BasePoint = new SuperPoint(1, 2, 3.0)
    val items = (superPoint, 1,2.0,(3,"4"))
    val tmpFile = temporaryFolder.newFile()
    items.toFile(tmpFile)
    val itemsSer = implicitly[TypedSerializer[(BasePoint, Int, Double, (Int, String))]]
    val itemsSer2 = serializerFromType[(BasePoint, Int, Double, (Int, String))] //another way to get the serializer
    val itemsFromFile = itemsSer.readItem(tmpFile)
    itemsFromFile should be (items)
  }

  //similarly to jsonFormat2 from spray
  def iso2[P1, P2, T <: Product]
  (construct: (P1, P2) => T): Iso[T, (P1, P2)] = new Iso[T, (P1, P2)] {
    override def from(input: T): (P1, P2) = {
      (input.productElement(0).asInstanceOf[P1],
        input.productElement(1).asInstanceOf[P2])
    }

    override def to(output: (P1, P2)): T = {
      construct(output._1, output._2)
    }
  }

  case class MyCaseClass(field1: Int, field2: String)
  case class NestedClass(n1: MyCaseClass, v: Double)

  test("serializer2") {
    val item = MyCaseClass(1, "one")
    implicit val myCaseClassIso = iso2(MyCaseClass)
    testSerializationImplicitly(item)

    val item2 = NestedClass(item, 2.5)
    implicit val nestedClassIso = iso2(NestedClass)
    testSerializationImplicitly(item2)
  }
}

