package com.stefansavev.randomprojections.datarepr.dense.store

import java.io.{BufferedInputStream, File, FileInputStream}

import com.stefansavev.core.serialization.core.DoubleArraySerializer
import com.stefansavev.randomprojections.actors.Application
import com.stefansavev.randomprojections.buffers._
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.implementation.HadamardUtils
import com.stefansavev.randomprojections.serialization.ValueStoreSerializationExt
import com.stefansavev.randomprojections.utils.Utils

import scala.reflect.ClassTag

trait ValuesStore {
  def fillRow(rowId: Int, output: Array[Double], isPos: Boolean): Unit

  def setRow(rowId: Int, input: Array[Double]): Unit

  def fillRow(rowId: Int, columnIds: Array[Int], output: Array[Double]): Unit

  def multiplyRowComponentWiseBySparseVector(rowId: Int, sv: SparseVector, output: Array[Double]): Unit

  def cosineForNormalizedData(query: Array[Double], id: Int): Double

  def getBuilderType: StoreBuilderType
}

object ValuesStore {

}

trait ValuesStoreBuilder {
  def getCurrentRowIndex: Int

  def isFull: Boolean

  def getContents(): Array[Byte]

  def addValues(values: Array[Double]): Unit

  def build(): ValuesStore

  def merge(numTotalRows: Int, valueStores: Iterator[ValuesStore]): ValuesStore
}

object ValuesStoreAsDoubleSerializationTags {
  val valuesStoreAsDouble = 1
  val valuesStoreAsBytes = 2
  val valuesStoreAsSingleByte = 3
  val lazyLoadValuesStore = 4
  val asyncLoadValuesStore = 5
}

class ValuesStoreBuilderAsFloat{
}


object FloatToByteEncoder{
  def encodeValue(value: Double): Short = {
    val intRepr: Int = java.lang.Float.floatToRawIntBits(value.toFloat)
    val s = (intRepr >>> 16).toShort
    s
  }

  def decodeValue(s: Short): Double = {
    val bits = (s & 0xFFFF) << 16
    java.lang.Float.intBitsToFloat(bits)
  }

}

object FloatToSingleByteEncoder{
  val almostZero = java.lang.Float.intBitsToFloat(1) //use this as a representation of zero: 00000000000000000000000000000001

  def getExponent(value:Double): Int = {
    val intRepr: Int = java.lang.Float.floatToRawIntBits(value.toFloat)
    val exp = (intRepr >>> (32 - 9)) & 0xFF //treat it as int from 0 to 255
    exp
  }

  def encodeValue(minValue: Float, maxValue: Float, value: Double): Byte = {
    val normalizedValue = (value - minValue)/(maxValue - minValue)  //from 0 to 1
    //if (normalizedValue < 0.0 || normalizedValue >= 1.0){
    //  println("normalizedValue: " + normalizedValue)
    //  Utils.internalError()
    //}
    //check if randomized rounding works better for similarity
    val rounded: Int = Math.min(255, (normalizedValue*255.0).round.toInt)
    if (rounded < 0 || rounded > 255){
      Utils.internalError()
    }
    val asByte = rounded.toByte
    val backToInt = asByte & 0xFF
    if (backToInt != rounded){
      Utils.internalError()
    }
    //println(value + " normalized: " + normalizedValue + " " + rounded + " " + asByte)
    asByte
  }

  def decodeValue(minValue: Float, maxValue: Float, input: Byte): Double = {
    val asInt = (input & 0xFF).toFloat
    val decoded: Float = (maxValue - minValue)*(asInt)/255.0f + minValue
    //println("decoded: " + decoded)
    decoded.toDouble
  }

  //need to take special care of zero
  def encodeValue_(minExp: Int, maxExp: Int, value: Double): Byte = {
    //println("input: " + value)
    //println("original exp: " + getExponent(value))
    val adjustedExp = getExponent(value) - minExp //normalized exp
    //println("exp: " + adjustedExp)
    val range = maxExp - minExp
    //println("range: " + range)
    val numBitsToUse = 32 - Integer.numberOfLeadingZeros(range)
    //println("#bits to use " + numBitsToUse)

    //val freeBits = 8 - numBitsToUse
    //println("normalized exp " + exp + " range: " + (maxExp - minExp) + " " + numBitsToUse)
    val intRepr: Int = java.lang.Float.floatToRawIntBits(value.toFloat)
    //println("intRepr " + allBits(intRepr))
    val signBit = (intRepr >>> 31)
    //println("signBit " + allBits(signBit))
    //println("diff exp: " + adjustedExp + " " + allBits(adjustedExp))
    val expRepr = ((adjustedExp) << 1)
    //println("exp " + allBits(expRepr))
    val availableBits = 8 - numBitsToUse - 1

    val rest = ((intRepr << 9) >>> (32 - availableBits)) << (1 + numBitsToUse)
    //println("rest " + allBits(rest))
    val output = rest | expRepr | signBit
    //println("output " + allBits(output))
    output.toByte
  }

  def decodeValue_(minExp: Int, maxExp: Int, input: Byte): Double = {
    //rest | exp bits | sign
    val intRepr = input & 0xFF
    //println("intRe  " + allBits(intRepr))
    val signBit = (intRepr & 0x1) << 31
    //println("signbit: " + allBits(signBit))
    val range = maxExp - minExp
    val numBitsToUse = 32 - Integer.numberOfLeadingZeros(range)
    val availableBits = 8 - numBitsToUse - 1

    val s = 1 << (numBitsToUse - 1)
    val mask = (s | (s - 1)) << 1
    //1 left (sign bit in compressed), 24 right , 1 left (again because of sign bit)
    //24 - 2
    val expRepr = ((intRepr & mask) >> 1)
    //println("mask: " + allBits(mask))
    //println("expBits: " + allBits(expRepr))

    val exp = expRepr + minExp
    //println("exp: " + exp)
    val shiftedExp = exp << 23
    //println("shifted exp: " + allBits(shiftedExp))
    val rest = (intRepr >>> (numBitsToUse + 1))  << (32 - 9 - availableBits)
    //println("rest " + allBits(rest))
    val rawBits = signBit | shiftedExp  | rest
    //println("rawbits out: " + allBits(rawBits))
    val finalResult = java.lang.Float.intBitsToFloat(rawBits)
    //println("finalRes: " + finalResult)
    //throw new IllegalStateException()
    finalResult
  }

  def encodeValues_(values: Array[Double], minExponentPerRecord: IntArrayBuffer, maxExponentPerRecord: IntArrayBuffer, valuesBuffer: ByteArrayBuffer): Unit = {
    //if we have zeros or values too close to zero this will increase the range of the exponent
    //need to check for the usual case (what is the range of the exponents)
    //but because of the normalization by the length, all numbers are smaller than 1

    var i = 0
    var minExp = 255
    var maxExp = 0
    while (i < values.length){
      val exp = getExponent(values(i))
      minExp = Math.min(exp, minExp)
      maxExp = Math.max(exp, maxExp)
      i += 1
    }

    minExponentPerRecord += minExp //.toByte
    maxExponentPerRecord += maxExp //.toByte
    i = 0
    while(i < values.length){
      valuesBuffer += encodeValue(minExp, maxExp, values(i))
      i += 1
    }
  }

  def encodeValues(values: Array[Double], minValues: FloatArrayBuffer, maxValues: FloatArrayBuffer, valuesBuffer: ByteArrayBuffer): Unit = {
    //if we have zeros or values too close to zero this will increase the range of the exponent
    //need to check for the usual case (what is the range of the exponents)
    //but because of the normalization by the length, all numbers are smaller than 1


    var minValue = values(0).toFloat
    var maxValue = minValue
    var i = 1
    while (i < values.length){
      val value = values(i).toFloat
      minValue = Math.min(minValue, value)
      maxValue = Math.max(maxValue, value)
      i += 1
    }

    minValues += minValue
    maxValues += maxValue
    i = 0
    while(i < values.length){
      valuesBuffer += encodeValue(minValue, maxValue, values(i))
      i += 1
    }
  }

  def encodeValues(offsetPerRecord: Int, minValues: Array[Float], maxValues: Array[Float], offsetValues: Int, valuesBuffer: Array[Byte], values: Array[Double]): Unit = {
    var minValue = values(0).toFloat
    var maxValue = minValue
    var i = 1
    while (i < values.length){
      val value = values(i).toFloat
      minValue = Math.min(minValue, value)
      maxValue = Math.max(maxValue, value)
      i += 1
    }
    minValues(offsetPerRecord) = minValue
    maxValues(offsetPerRecord) = maxValue
    i = 0
    var k = offsetValues
    while(i < values.length){
      valuesBuffer(k) = encodeValue(minValue, maxValue, values(i))
      i += 1
      k += 1
    }
  }

  def decodeValues(offsetPerRecord: Int, minValues: Array[Float], maxValues: Array[Float], offsetValues: Int, valuesBuffer: Array[Byte], output: Array[Double]): Unit = {
    val minValue = minValues(offsetPerRecord)
    val maxValue = maxValues(offsetPerRecord)
    var k = offsetValues
    var j = 0
    var i = 0
    while(i < output.length){
      val encoded = valuesBuffer(k)
      val decoded = decodeValue(minValue, maxValue, encoded)
      output(i) = decoded
      i += 1
      j += 1
      k += 1
    }
  }
}

class FixedLengthBuffer[T : ClassTag](val size: Int){
  val buffer = Array.ofDim[T](size)
  var offset = 0
  def ++= (values: Array[T]): Unit = {
    System.arraycopy(values, 0, buffer, offset, values.length)
    offset += values.length
  }
  def array = buffer
}






