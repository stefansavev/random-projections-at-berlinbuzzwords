package com.stefansavev

import java.nio.ByteBuffer
import java.util.Random

import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.utils.RandomUtils
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable.ArrayBuffer

class SignaturesSpec extends FlatSpec with Matchers {

  def computeSignaturesFromScratch(rnd: Random, dataFrameView: DataFrameView, numSig: Int): Array[Array[Double]] = {
    val repeat = numSig*64
    val vecs = ArrayBuffer[Array[Double]]()

    for(j <- 0 until repeat){
      val v = RandomUtils.generateRandomVector(rnd, dataFrameView.numCols, Array.range(0, dataFrameView.numCols))
      vecs += v.values
    }

    val dataFrameViewOutput = ArrayBuffer[Array[Double]]()
    for(i <- 0 until dataFrameView.numRows){
      val row = dataFrameView.getPointAsDenseVector(i)
      val output = ArrayBuffer[Double]()
      for(j <- 0 until repeat){
        val dp = dotProduct(row, vecs(j))
        val roundedDp = if (dp >= 0.0) 1.0 else -1.0
        output += roundedDp
      }
      dataFrameViewOutput += (output.toArray)
    }
    dataFrameViewOutput.toArray
  }

  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    var sum = 0.0
    var num1 = 0.0
    var num2 = 0.0
    var i = 0
    while(i < v1.length){
      val a = v1(i)
      val b = v2(i)
      sum += a*b
      num1 += a*a
      num2 += b*b
      i += 1
    }
    sum
  }

  def overlapVectors(v1: Array[Double], v2: Array[Double]): Double = {
    var sum = 0.0
    var i = 0
    while(i < v1.length){
      val a = v1(i)
      val b = v2(i)
      val s = if (a == b) 1.0 else 0.0
      sum += s

      i += 1
    }
    sum
  }

  def cosineFromOverlap(overlap: Double, numSig: Int): Double = {
    val numSigBits = 64.0*numSig.toDouble //number of bits in signature
    val p = overlap.toDouble / numSigBits
    val angle = (1.0 - p)*Math.PI
    Math.cos(angle)
  }

  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    var sum = 0.0
    var num1 = 0.0
    var num2 = 0.0
    var i = 0
    while(i < v1.length){
      val a = v1(i)
      val b = v2(i)
      sum += a*b
      num1 += a*a
      num2 += b*b
      i += 1
    }
    num1 = Math.sqrt(num1)
    num2 = Math.sqrt(num2)
    val num = num1 * num2
    if (num < 1e-5){
      0.0
    }
    else{
      sum/num
    }
  }

  val dataGenSettings = RandomBitStrings.RandomBitSettings(
    numGroups = 1000,
    numRowsPerGroup = 2,
    numCols=256,
    per1sInPrototype = 0.5,
    perNoise = 0.2)

  val debug = false
  val randomBitStringsDataset = RandomBitStrings.genRandomData(58585, dataGenSettings, debug, true)
  val rnd = new Random(48181671)
  val numSignatures = 16 //64
  randomBitStringsDataset.buildSetSignatures(numSignatures, rnd)
  val fromScratch = computeSignaturesFromScratch(rnd, randomBitStringsDataset, numSignatures)

  def generateTestFile(): Unit = {
    val fileName = "D:/tmp/cos-overlap.txt"
    val buff = ArrayBuffer[String]()
    for(i <- 0 until dataGenSettings.numGroups) {
      val (p1, p2) = (2 * i, 2 * i + 1)
      val q1 = randomBitStringsDataset.getPointAsDenseVector(p1)
      val q2 = randomBitStringsDataset.getPointAsDenseVector(p2)
      val cosValue = cosine(q1, q2)
      val overlap = randomBitStringsDataset.pointSignatures.overlapTwoPoints(p1, p2, numSignatures)

      val s1 = fromScratch(p1)
      val s2 = fromScratch(p2)
      val overlapFromScratch = overlapVectors(s1, s2) //should be equal to overlap in this case

      val estCosine = randomBitStringsDataset.pointSignatures.estimatedCosineTwoPoints(p1, p2, numSignatures)
      buff += (cosValue + "\t" + overlap + "\t" + estCosine + "\t" + overlapFromScratch)
    }
    PrintUtils.stringsToFile(fileName, buff.toArray)
  }

  //can be used during development to see plots of the data
  //generateTestFile() //generate a file for analysis in R (see resources/test-signagures.R)

  def estimateAvgError(): (Double, Double) = {
    var errorSum = 0.0
    var errorSumFromScratch = 0.0
    for(i <- 0 until dataGenSettings.numGroups) {
      val (p1, p2) = (2 * i, 2 * i + 1)
      val q1 = randomBitStringsDataset.getPointAsDenseVector(p1)
      val q2 = randomBitStringsDataset.getPointAsDenseVector(p2)
      val cosValue = cosine(q1, q2)
      val estCosine = randomBitStringsDataset.pointSignatures.estimatedCosineTwoPoints(p1, p2, numSignatures)

      val s1 = fromScratch(p1)
      val s2 = fromScratch(p2)
      val overlapFromScratch = overlapVectors(s1, s2) //should be equal to overlap in this case
      val estCosineFromScratch = cosineFromOverlap(overlapFromScratch, numSignatures)
      //println(cosValue + "\t" + estCosine + "\t" + estCosineFromScratch)
      val err = Math.abs(cosValue - estCosine)
      errorSum += err
      errorSumFromScratch += Math.abs(cosValue - estCosineFromScratch)
    }
    val avgError = errorSum/dataGenSettings.numGroups.toDouble
    val avgErrorFromScratch = errorSumFromScratch/dataGenSettings.numGroups.toDouble
    (avgError, avgErrorFromScratch)
  }

  "Error between truth and estimation" should "be small" in {
    val (err, errFromScratch) = estimateAvgError()
    println(s"error between cosine and signature estimation (also error from scratch implementation): ${err} ${errFromScratch}")
    (err < 0.04) should be (true)
    (errFromScratch < 0.04) should be (true)
  }
}