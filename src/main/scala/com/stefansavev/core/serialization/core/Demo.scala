package com.stefansavev.core.serialization.core

object Demo {
  import Core._
  import PrimitiveTypeSerializers._
  import TupleSerializers._

  def roundTripInputOutput[A](ser: TypedSerializer[A], input: A, fileName: String): A = {
    toFile(ser, fileName, input)
    val output = fromFile(ser, fileName)
    println(output)
    output
  }

  case class PointXY(x: Int, y: Double)

  case class BasePoint(x: Int, y: Double)

  class ExtendedPoint(val z: Int, x: Int, y: Double) extends BasePoint(x, y){
    override def toString = "ExtendedPoint" + (z, x, y)
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

  implicit object PointXYIso extends Iso[PointXY, (Int, Double)]{

    def from(input: PointXY): Output = (input.x, input.y)

    def to(input: Output): PointXY = {
      PointXY(input._1, input._2)
    }
  }

  implicit object ExtendedPointIso extends Iso[ExtendedPoint, (Int, Int, Double)]{
    def from(input: Input): Output = (input.z, input.x, input.y)
    def to(output: Output): Input = new ExtendedPoint(output._1, output._2, output._3)
  }

  implicit object SuperPointIso extends Iso[SuperPoint, (Int, Int, Double)]{
    def from(input: Input): Output = (input.k, input.x, input.y)
    def to(output: Output): Input = new SuperPoint(output._1, output._2, output._3)
  }

  implicit def basePointSer(): TypedSerializer[BasePoint] = {
    subtype2Serializer[BasePoint, ExtendedPoint, SuperPoint]
  }

  //class PointXYSerializer extends IsoSerializer[PointXY,(Int, Double)](PointXYIso, Tuple2Serializer[Int, Double])
  implicit def pointXYSerializer(): TypedSerializer[PointXY] = {
    isoSerializer[PointXY, (Int,Double)]
  }

  def main (args: Array[String]): Unit = {
    val items = (1,2.0)
    val tmpFile = "D:/tmp/somefile-ser.txt"
    val ser = tuple2Serializer[Int, Double]
    roundTripInputOutput(ser, items, tmpFile)

    val items2 = (1,(2.0, 3))
    val ser2 = tuple2Serializer[Int, (Double, Int)]
    roundTripInputOutput(ser2, items2, tmpFile)

    val pointXY = PointXY(1, 3.4)
    val ser3 = pointXYSerializer()
    roundTripInputOutput(ser3, pointXY, tmpFile)

    val items3 = ((1,2), PointXY(1,3.8))
    val ser4 = tuple2Serializer[(Int, Int), PointXY]
    roundTripInputOutput[((Int, Int), PointXY)](ser4, items3, tmpFile) //((Int, Int), PointXY) is nice for debugging the types
    roundTripInputOutput(ser4, items3, tmpFile)

    val extPoint: BasePoint = new ExtendedPoint(1,2,3.0)
    val ser5 = basePointSer()
    roundTripInputOutput[BasePoint](ser5, extPoint, tmpFile)
    val superPoint: BasePoint = new SuperPoint(1,2,3.0)
    roundTripInputOutput[BasePoint](ser5, superPoint, tmpFile)
  }
}
