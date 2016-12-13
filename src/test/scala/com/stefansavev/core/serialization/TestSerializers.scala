package com.stefansavev.core.serialization

import java.io.File

import com.stefansavev.TemporaryFolderFixture
import com.stefansavev.core.serialization.IsoSerializers._
import com.stefansavev.core.serialization.PrimitiveTypeSerializers._
import com.stefansavev.core.serialization.SubtypeSerializers._
import com.stefansavev.core.serialization.TupleSerializers._
import com.stefansavev.core.serialization.Utils._
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
    override def toString = "ExtendedPoint" + (z, x, y)
  }

  class SuperPoint(val k: Int, x: Int, y: Double) extends ExtendedPoint(-1, x, y) {
    override def toString = "SuperPoint" + (z, x, y)
  }

  implicit object ExtendedPointTag extends TypeTag[ExtendedPoint] {
    def tag: Int = 2
  }

  implicit object SuperPointTag extends TypeTag[SuperPoint] {
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

  test("implicit serializers") {
    val items = (1, 2.0, (3, "4"))
    testSerialization(items, implicitly[TypedSerializer[(Int, Double, (Int, String))]])
    testSerializationImplicitly(items)
  }

  test("array of ints") {
    val arr = Array(1, 2, 3)
    testSerializationImplicitly(arr)
  }

  implicit class ToFileExtension[A](item: A) {
    def toFile(outputFile: File)(implicit ser: TypedSerializer[A]): Unit = {
      import Utils.{toFile => toFileImpl}
      toFileImpl(ser, outputFile, item)
    }
  }

  implicit class ItemSerializerExt[T](ser: TypedSerializer[T]) {
    def readItem(file: File): T = {
      fromFile(ser, file)
    }
  }

  test("serialization extensions") {
    val superPoint: BasePoint = new SuperPoint(1, 2, 3.0)
    val items = (superPoint, 1, 2.0, (3, "4"))
    val tmpFile = temporaryFolder.newFile()
    items.toFile(tmpFile)
    val itemsSer = implicitly[TypedSerializer[(BasePoint, Int, Double, (Int, String))]]
    val expectedName = "tuple4(subtype2(IsoSerializer(via = tuple3(int, int, double)), " +
      "IsoSerializer(via = tuple3(int, int, double))), int, double, tuple2(int, string))"

    (itemsSer.name) should be(expectedName)
    val itemsSer2 = serializerFromType[(BasePoint, Int, Double, (Int, String))] //another way to get the serializer
    val itemsFromFile = itemsSer.readItem(tmpFile)
    itemsFromFile should be(items)
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

  test("basetypes") {
    val i: Int = 11
    val b: Byte = 0x1
    val s: Short = 1.toShort
    val li: Long = 13L
    val d: Double = 1.1
    val f: Float = 1.5f
    val str: String = "a string"

    testSerializationImplicitly(i)
    testSerializationImplicitly(b)
    testSerializationImplicitly(s)
    testSerializationImplicitly(li)
    testSerializationImplicitly(d)
    testSerializationImplicitly(f)
    testSerializationImplicitly(str)

    val allInOne = (i, b, s, li, d, f, str)
    testSerializationImplicitly(allInOne)

    val tuplesOfTuples = (i, ((b, (s, li)), ((d, f), str)))
    testSerializationImplicitly(tuplesOfTuples)
  }

  trait TypePicker[A] {
    def name: String
  }

  class IntTypePicker(arr: Array[Int]) extends TypePicker[Array[Int]] {
    def name: String = "array(int)"
  }

  def pickSerializer[A](item: A)(implicit ser: TypedSerializer[A]): TypedSerializer[A] = {
    ser
  }

  test("array of basetypes") {
    import GenericArraySerializer._

    val i: Array[Int] = Array(11)
    val b: Array[Byte] = Array(0x1.toByte)
    val s: Array[Short] = Array(1.toShort)
    val li: Array[Long] = Array(13L)
    val d: Array[Double] = Array(1.1)
    val f: Array[Float] = Array(1.5f)
    val str: Array[String] = Array("a string")

    testSerializationImplicitly(i)
    testSerializationImplicitly(b)
    testSerializationImplicitly(s)
    testSerializationImplicitly(li)
    testSerializationImplicitly(d)
    testSerializationImplicitly(f)
    testSerializationImplicitly(str)

    val allInOne = (i, b, s, li, d, f, str)
    val ser = pickSerializer(allInOne)
    val expectedSerializerName = "tuple7(array[int], array[byte], array[short], array[long], array[double], " +
      "array[float], GenericArraySerializer(string))"

    ser.name should be(expectedSerializerName)

    val output = roundTripInputOutput(ser, allInOne, temporaryFolder.newFile())

    //convert the nested arrays to string manually and then compare
    def convertTuple(t: Product): String =
    t.productIterator.map { element => element match {
      case a: Array[_] => a.mkString(";")
      case _ => element.toString()
    }
    }.mkString(", ")
    convertTuple(allInOne) should be(convertTuple(output))
  }

  test("array of arrays of primitives") {
    import GenericArraySerializer._
    val i: Array[Array[Int]] = Array(Array(11), Array(12), Array(13))
    val b: Array[Array[Byte]] = Array(Array(0x1.toByte))
    val s: Array[Array[Short]] = Array(Array(5.toShort))
    val li: Array[Array[Long]] = Array(Array(13L))
    val d: Array[Array[Double]] = Array(Array(1.1))
    val f: Array[Array[Float]] = Array(Array(1.5f))
    val str: Array[Array[String]] = Array(Array("a string"))

    val allInOne = (i, b, s, li, d, f, str)
    val ser = pickSerializer(allInOne)
    val expectedName = "tuple7(GenericArraySerializer(array[int]), GenericArraySerializer(array[byte]), " +
      "GenericArraySerializer(array[short]), GenericArraySerializer(array[long]), " +
      "GenericArraySerializer(array[double]), GenericArraySerializer(array[float]), " +
      "GenericArraySerializer(GenericArraySerializer(string)))"

    val output = roundTripInputOutput(ser, allInOne, temporaryFolder.newFile())
    def convertTuple(t: Product): String = {
      def formatElement(v: Any): String = v + ":" + v.getClass.getSimpleName
      "(" + t.productIterator.map { element => element match {
        case a: Array[Array[_]] => "[" + a.map(b => "[" + b.map(formatElement(_)).mkString + "]").mkString(",") + "]"
        case _ => (element.toString()) + ":" + element.getClass.getSimpleName
      }
      }.mkString(", ") + ")"
    }
    ser.name should be(expectedName)

    val expectedOutput = "([[11:Integer],[12:Integer],[13:Integer]], [[1:Byte]], [[5:Short]], [[13:Long]], " +
      "[[1.1:Double]], [[1.5:Float]], [[a string:String]])"
    convertTuple(output) should be(expectedOutput)
  }
}

