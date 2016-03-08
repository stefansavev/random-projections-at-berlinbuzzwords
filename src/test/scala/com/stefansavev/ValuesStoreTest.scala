package com.stefansavev

import java.util.Random

import com.stefansavev.randomprojections.datarepr.dense.store._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class TestSingleByteEncodingSpec extends FlatSpec with Matchers {
  "Error after encoding double to float" should "be small" in {
    val minV = -1.0f
    val maxV = 2.0f
    val rnd = new Random(481861)
    for (i <- 0 until 100) {
      //we encode a float (which is 4 bytes) with a single byte
      //therefore the loss of precision
      val value = rnd.nextFloat() * 3.0f - 1.0f
      val enc = FloatToSingleByteEncoder.encodeValue(minV, maxV, value)
      val dec = FloatToSingleByteEncoder.decodeValue(minV, maxV, enc)
      val error = Math.abs(value - dec)
      error should be < (0.01)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class TestValueStores extends FlatSpec with Matchers {

  case class BuilderTypeWithErrorPredicate(builderType: StoreBuilderType, pred: Double => Boolean)

  "ValueStore" should "return store the data with small error" in {

    val tests = List(
      BuilderTypeWithErrorPredicate(StoreBuilderAsDoubleType, error => (error <= 0.0)),
      BuilderTypeWithErrorPredicate(StoreBuilderAsBytesType, error => (error <= 0.01)),
      BuilderTypeWithErrorPredicate(StoreBuilderAsSingleByteType, error => (error <= 0.01))
    )

    for (test <- tests) {
      testBuilder(test)
    }

    def testBuilder(builderWithPred: BuilderTypeWithErrorPredicate): Unit = {
      val dataGenSettings = RandomBitStrings.RandomBitSettings(
        numGroups = 1000,
        numRowsPerGroup = 2,
        numCols = 256,
        per1sInPrototype = 0.5,
        perNoise = 0.2)

      val debug = false
      val randomBitStringsDataset = RandomBitStrings.genRandomData(58585, dataGenSettings, debug, true)
      val builder = builderWithPred.builderType.getBuilder(randomBitStringsDataset.numCols)

      def addValues(): Unit = {
        var i = 0
        while (i < randomBitStringsDataset.numRows) {
          val values = randomBitStringsDataset.getPointAsDenseVector(i)
          builder.addValues(values)
          i += 1
        }
      }

      addValues()

      val valueStore = builder.build()

      def verifyStoredValues(expected: Array[Double], stored: Array[Double]): Unit = {
        for (i <- 0 until expected.length) {
          val error = Math.abs(expected(i) - stored(i))
          val passed = builderWithPred.pred(error)
          passed should be (true)
        }
      }

      def testValues(): Unit = {
        var i = 0
        while (i < randomBitStringsDataset.numRows) {
          val values = randomBitStringsDataset.getPointAsDenseVector(i)
          val output = Array.ofDim[Double](randomBitStringsDataset.numCols)
          valueStore.fillRow(i, output, true)
          verifyStoredValues(values, output)
          i += 1
        }
      }
      testValues()
    }
  }
}
