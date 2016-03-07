package com.stefansavev

import com.stefansavev.randomprojections.datarepr.dense.store.FloatToSingleByteEncoder

object TestSingleByteEnc{
  def main(args: Array[String]) {
    val minV = -1.0f
    val maxV = 2.0f
    for(i <- 0 until 100) {
      val value = scala.util.Random.nextFloat()*3.0f - 1.0f
      val enc = FloatToSingleByteEncoder.encodeValue(minV, maxV, value)
      val dec = FloatToSingleByteEncoder.decodeValue(minV, maxV, enc)
      println("enc-dec-error: " + value + " " + dec + " " + (value - dec))
    }

  }
}
