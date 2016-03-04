package com.stefansavev.randomprojections.evaluation

import com.stefansavev.similaritysearch.{SimilaritySearchQueryResults, SimilaritySearchResults}
import com.stefansavev.randomprojections.implementation.{OnlineVariance, KNNS}
import com.stefansavev.randomprojections.utils.Utils

object RecallEvaluator{
  class Recall(numTotalEvaluationQueries: Int, numRetrievedQueries: Int, recalls: Array[Double], recallStd: Array[Double]){
    def printRecalls(): Unit = {
      println("------------------------------")
      println(s"Total queries: " + numTotalEvaluationQueries)
      val notRetrieved = numTotalEvaluationQueries - numRetrievedQueries
      println(s"Not-retrieved queries: " + notRetrieved)
      println(s"Recalls from ${numRetrievedQueries} queries")
      for(i <- 0 until recalls.length){
        val recall = recalls(i)
        println(s"Recall@${i}: ${recall}")
      }
    }
  }

  def evaluateRecall(maxCutoff: Int, retrieved: SimilaritySearchResults, expected: SimilaritySearchResults): Recall = {
    import scala.collection.JavaConversions._

    if (maxCutoff > 100){
      Utils.failWith("Invalid cutoff. Should be <= 100")
    }

    val onlineRecalls = new OnlineVariance(maxCutoff)

    def evaluateQuery(name: String, retrievedQuery: SimilaritySearchQueryResults, expectedQuery: SimilaritySearchQueryResults): Unit = {
      if (expectedQuery.getQueryResults.size() < maxCutoff){
        Utils.failWith("The query is expected to have at least maxCutoff answers")
      }

      val recalls =
        (for(i <- 1 until (maxCutoff + 1)) yield{
          //remove the query from both result lists
          val topKExpected = expectedQuery.getQueryResults.map(n => n.getName).filter(_ != name).take(i).toSet
          val topKRetrieved = retrievedQuery.getQueryResults.map(n => n.getName).filter(_ != name).take(i).toSet
          val overlap = topKExpected.intersect(topKRetrieved).size.toDouble
          val maxOverlap = topKExpected.size.toDouble
          val recall = 100.0*overlap/maxOverlap
          recall
        }).toArray
      onlineRecalls.processPoint(recalls)
    }

    var numRetrieved = 0
    var nonRetrieved = 0
    for(expectedResult <- expected.getIterator){
      val name = expectedResult.getName
      if (retrieved.hasQuery(name)){
        val retrievedResult = retrieved.getQueryByName(name)
        if (retrievedResult.getQueryResults.size() >= maxCutoff){
          evaluateQuery(name, retrievedResult, expectedResult)
          numRetrieved += 1
        }
        else{
          nonRetrieved += 1
        }
      }
      else{
        nonRetrieved += 1
      }
    }
    val (means, variances) = onlineRecalls.getMeanAndVar()
    val stdDev = variances.map(v => Math.sqrt(v))
    new Recall(numRetrieved + nonRetrieved, numRetrieved, means, stdDev)
  }

  def evaluateRecall(maxCutoff: Int, retrieved: Array[KNNS], expected: Array[KNNS]): Recall = {
    if (maxCutoff > 100){
      Utils.failWith("Invalid cutoff. Should be <= 100")
    }

    val onlineRecalls = new OnlineVariance(maxCutoff)

    def evaluateQuery(retrievedQuery: KNNS, expectedQuery: KNNS): Unit = {
      if (retrievedQuery.pointId != expectedQuery.pointId){
        Utils.failWith("Mismatch between retrievedQuery and expectedQuery")
      }
      if (expectedQuery.neighbors.length < maxCutoff){
        Utils.failWith("The query is expected to have at least maxCutoff answers")
      }

      val recalls =
        (for(i <- 1 until (maxCutoff + 1)) yield{
          val topKExpected = expectedQuery.neighbors.map(n => n.neighborId).take(i).toSet
          val topKRetrieved = retrievedQuery.neighbors.map(n => n.neighborId).take(i).toSet
          val overlap = topKExpected.intersect(topKRetrieved).size.toDouble
          val maxOverlap = topKExpected.size.toDouble
          val recall = 100.0*overlap/maxOverlap
          recall
        }).toArray
      onlineRecalls.processPoint(recalls)
    }

    retrieved.toSeq.zip(expected.toSeq).foreach({case (retrievedQuery, expectedQuery) => {
      evaluateQuery(retrievedQuery, expectedQuery)
    }})
    val (means, variances) = onlineRecalls.getMeanAndVar()
    val stdDev = variances.map(v => Math.sqrt(v))
    new Recall(expected.length, retrieved.length, means, stdDev)
  }
}
