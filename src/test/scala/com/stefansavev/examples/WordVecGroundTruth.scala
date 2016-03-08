package com.stefansavev.examples

import java.io.PrintWriter

import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.evaluation.Evaluation
import com.stefansavev.randomprojections.implementation.KNNS

import scala.io.Source

object WordVecGroundTruth {
  case class QueryAnswerScore(query: String, answer: String, score: Double)
  case class GroundTruth(mapping: Map[String, Vector[(String, Double)]])

  def readGroundTruthInObject(): GroundTruth= {
    GroundTruth(readGroundTruth("D:\\RandomTreesData-144818512896186816\\output\\parsed_nn.txt\\parsed_nn.txt"))
  }

  def readGroundTruth(fileName: String): Map[String, Vector[(String, Double)]] = {
    //D:\RandomTreesData-144818512896186816\output\parsed_nn.txt\parsed_nn.txt
    val file = Source.fromFile(fileName)
    val linesIter = file.getLines()

    val numWordsPattern = """num_words:\s+(\d+)""".r

    def readNumWords(): Int = {
      val line = linesIter.next()
      line match {
        case numWordsPattern(value) => value.toInt
      }
    }

    val queryPattern = """query:\s+(\w+)\s+(\d+)""".r

    /*
    def readQuery(): Option[(String, Int)] = {
      if (!linesIter.hasNext){
        None
      }
      else{
        val line = linesIter.next()
        line match {
          case queryPattern(query,wordId) => Some((query, wordId.toInt))
          case _ => None
        }
      }
    }
    */

    /*
    def readQueryResult(): Option[(String, String, Double)] = {
      if (!linesIter.hasNext){
        None
      }
      else{
        val line = linesIter.next()
        line match {
          case resultPattern(word, wordId, nn, score) => Some((word, nn, score.toDouble))
          case _ => None
        }
      }
    }
    */

    val resultPattern = """nn:\s+(.*?)\s+(.*?)\s+(.*?)\s+(.*?)""".r
    def matchPattern(line: String): QueryAnswerScore = {
      val parts = line.split(" ")
      val word = parts(1)
      val nn = parts(3)
      val score = parts(4)
      QueryAnswerScore(word, nn, score.toDouble)
      /*
      line match {
        case resultPattern(word, wordId, nn, score) => QueryAnswerScore(word, nn, score.toDouble)
        case _ => Utils.failWith("Could not match query result for line " + line)
      }
      */
    }

    val numWords = readNumWords()
    val queryAnswerScore = linesIter.filter(line => !line.startsWith("query:")).map(line => matchPattern(line)).toVector
    println("parsed answers")
    val results = queryAnswerScore.groupBy(_.query).map{case (query,results)=> (query, results.sortBy(- _.score).take(100).map(a => (a.answer, a.score)) )}
    file.close()
    results
  }

  def evaluateRecall(fileName: String, knns: Array[KNNS], labels: Array[String], kNeigbhor: Int, expected: Map[String, Vector[(String, Double)]], kExpected:  Int): Double = {
    val labelsSet = labels.toSet
    var missing = 0
    var present = 0.0
    var avgRetrieved = 0.0
    val writer = new PrintWriter(fileName)
    var total = 0
    for((lab_i, knn) <- labels.zip(knns)){
      val other_knns = knn.neighbors.filter(nn => nn.neighborId != knn.pointId).take(kNeigbhor)
      if (other_knns.length < kNeigbhor){
        missing += 1
      }

      val retrieved = other_knns.map({case n => labels(n.neighborId)}).toSet
      val expectedRetrieved = expected(lab_i)
      var numRetrieved = 0.0
      val expectedAnswers = expectedRetrieved.filter({case (name,score)=> labelsSet.contains(name)}).take(kExpected)
      for(answer <- expectedAnswers){
        if (retrieved.contains(answer._1)){
          numRetrieved += 1
        }
      }
      writer.write(lab_i + " " + numRetrieved + " " + expectedAnswers.length + "\n")
      if (expectedAnswers.length > 0) {
        avgRetrieved += (numRetrieved / (expectedAnswers.length.toDouble))
        total += 1
      }
    }
    avgRetrieved /= total
    println("Recall " + (100.0*avgRetrieved))
    writer.close()
    100.0*avgRetrieved
  }

  def evaluateRecall(allNN: Array[KNNS], dataset: DataFrameView): Unit = {
    println("reading ground truth")
    val truth = readGroundTruth("D:\\RandomTreesData-144818512896186816\\output\\parsed_nn.txt\\parsed_nn.txt")
    evaluateRecall("D:/tmp/word-vec-recall.txt", allNN, dataset.rowStoredView.getAllRowNames(), 10,truth, 10)
  }

  def evaluateRecall(allNN: Array[KNNS], groundTruth: GroundTruth,  dataset: DataFrameView, k: Int = 10): Double = {
    //println("reading ground truth")
    val truth = groundTruth.mapping// readGroundTruth("D:\\RandomTreesData-144818512896186816\\output\\parsed_nn.txt\\parsed_nn.txt")
    evaluateRecall("D:/tmp/word-vec-recall.txt", allNN, dataset.rowStoredView.getAllRowNames(), k,truth, k)
  }

  def stringsToFile(fileName: String, v: Array[String]): Unit = {
    val writer = new PrintWriter(fileName)
    for(a <- v){
      writer.println(a)
    }
    writer.close()
  }

  def main (args: Array[String]) {
    val truth = readGroundTruth("D:\\RandomTreesData-144818512896186816\\output\\parsed_nn.txt\\parsed_nn.txt")
    //take the score of the 10th NN
    val asPairs = truth.toVector.map({case (word, neighbors) => (word, neighbors.take(10).reverse.take(1).map({case (n,score) => score}).sum)}).sortBy(-_._2).toArray
    val asStrings = asPairs.map({case (word, sumOfScores: Double) => word + " " + sumOfScores})
    stringsToFile("D:/tmp/exact-neigborhoods.txt", asStrings)
    val asScores = asPairs.map{case (word, sumOfScores) => sumOfScores}
  }


}
