package com.stefansavev.fuzzysearchtest

import java.io.StringReader

import com.stefansavev.TemporaryFolderFixture
import com.stefansavev.core.serialization.TupleSerializers._
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GloveUnitTest extends FunSuite with TemporaryFolderFixture with Matchers {

  def readResource(name: String): String ={
    val stream = getClass.getResourceAsStream(name)
    val lines = scala.io.Source.fromInputStream( stream ).getLines
    lines.mkString("\n")
  }

  def parameterizedTest(inputTextFile: String, indexFile: String, numTrees: Int, expectedResultsName: String): Unit ={
    val expectedResults = readResource(expectedResultsName).trim
    val queryResults = GloveTest.run(inputTextFile, indexFile, numTrees).trim
    assertResult(expectedResults)(queryResults)
  }

  //manually download http://nlp.stanford.edu/data/glove.6B.zip and unzip into test/resources/glove
  //then enable the test
  ignore("test glove num trees 1") {
    val numTrees: Int = 1
    val inputTextFile: String = "src/test/resources/glove/glove.6B.100d.txt"
    val index = temporaryFolder.newFolder("index").getAbsolutePath
    val expectedResultsResouceName = "/glove/expected_results_num_trees_1.txt"
    parameterizedTest(inputTextFile, index, numTrees, expectedResultsResouceName)
  }

  ignore("test glove num trees 150") {
    val numTrees: Int = 150
    val inputTextFile: String = "src/test/resources/glove/glove.6B.100d.txt"
    val index = temporaryFolder.newFolder("index").getAbsolutePath
    val expectedResultsResouceName = "/glove/expected_results_num_trees_150.txt"
    parameterizedTest(inputTextFile, index, numTrees, expectedResultsResouceName)
  }
}