package com.stefansavev.fuzzysearch.implementation

import java.io._
import java.util

import com.stefansavev.fuzzysearch.{FuzzySearchQueryResults, FuzzySearchResult}
import com.stefansavev.randomprojections.utils.{DirectIndexWithSimilarity, DirectIndexWithSimilarityBuilder, AddStringResult, String2UniqueIdTable}

import scala.io.Codec

object FuzzySearchResultsWrapper{
  def fromTextFile(fileName: String): FuzzySearchResultsWrapper = {
    val source = scala.io.Source.fromFile(new File(fileName), Codec.UTF8.charSet.name())
    var line = 0
    val iter = source.getLines()
    val firstLine = iter.next()
    if (!firstLine.startsWith("*****")){
      throw new IllegalStateException("Line 1 should start with *****")
    }
    val builder = new FuzzySearchResultsBuilderWrapper()
    while(iter.hasNext){
      val queryLine = iter.next()
      line += 1
      val queryParts = queryLine.split("\t")
      val queryStr = queryParts(1)
      if (queryStr != "query:"){
        throw new IllegalStateException(s"Line should contain the token 'query:'")
      }
      val queryName = queryParts(2)
      var hasMoreResults = true
      val resultsArray = new java.util.ArrayList[FuzzySearchResult]()
      while(iter.hasNext && hasMoreResults){
        val result = iter.next()
        line += 1
        if (result.startsWith("*****")){
          hasMoreResults = false
        }
        else{
          if (!result.startsWith("\t")){
            throw new IllegalStateException(s"Line ${line} should start with tab")
          }
          val parts = result.split("\t")
          //parts[0] is empty
          val index = parts(1) //is the index, should be consequtive
          val resultName = parts(2)
          val cosine = parts(3).toDouble //this is the cosine
          resultsArray.add(new FuzzySearchResult(resultName, -1, cosine))
        }
      }
      builder.addResult(queryName, resultsArray)
    }
    source.close()
    builder.build()
  }
}

class FuzzySearchResultsWrapper(queryString2Id: String2UniqueIdTable, resultsString2Id: String2UniqueIdTable, directIndex: DirectIndexWithSimilarity){

  def getIterator: java.util.Iterator[FuzzySearchQueryResults] = {
    import scala.collection.JavaConversions._
    val numQueries = getNumberOfQueries
    Iterator.range(0, numQueries).map(i => getQueryAtPos(i))
  }

  def getNumberOfQueries: Int = {
    return directIndex.numDataPoints()
  }

  def hasQuery(name: String): Boolean = {
    (queryString2Id.getStringId(name) >= 0)
  }

  def getQueryByName(name: String): FuzzySearchQueryResults = {
    val pos = queryString2Id.getStringId(name)
    if (pos < 0){
      throw new IllegalStateException("Query does not exist in results set " + name)
    }
    getQueryAtPos(pos)
  }

  def getQueryAtPos(pos: Int): FuzzySearchQueryResults = {
    val queryName = queryString2Id.getStringById(pos)
    val (neighborIds, similarities) = directIndex.getContextIdsAndSimilarities(pos)
    val results = new util.ArrayList[FuzzySearchResult]()
    var i = 0
    while(i < neighborIds.length){
      val resultName = resultsString2Id.getStringById(neighborIds(i))
      val result = new FuzzySearchResult(resultName, -1, similarities(i))
      results.add(result)
      i += 1
    }
    return new FuzzySearchQueryResults(queryName, results)
  }


  def toTextFile(fileName: String): Unit = {
    val writer = new PrintWriter(fileName, Codec.UTF8.charSet.name())
    val numQueries = getNumberOfQueries
    for(i <- 0 until numQueries) {
      val queryResults = getQueryAtPos(i)
      writer.write("************************************\n")
      writer.write((i + 1) + "\tquery:\t" + queryResults.getName + "\n")
      var j = 0
      val results = queryResults.getQueryResults
      while(j < results.size()) {
        val result = results.get(j)
        writer.write("\t" + (j + 1) + "\t" + result.getName + "\t" + result.getCosineSimilarity + "\n")
        j += 1
      }
    }
    writer.close()
  }

}

class FuzzySearchResultsBuilderWrapper {
  val addStringResult = new AddStringResult()

  val queryString2Id = new String2UniqueIdTable() //TODO: want to have only one table with strings
  val resultsString2Id = new String2UniqueIdTable()
  val directIndexBuilder = new DirectIndexWithSimilarityBuilder()

  def addResult(queryId: String, resultList: java.util.List[FuzzySearchResult]): Unit = {
    queryString2Id.addString(queryId, addStringResult)
    if (addStringResult.existedPreviously){
      throw new IllegalStateException(s"Result for query ${queryId} has already been added and cannot be added again")
    }
    val queryIntegerId = addStringResult.stringId
    //println("queryId / intid" + queryId +  " " + queryIntegerId)
    val resultNameIds = Array.ofDim[Int](resultList.size)
    val similarities = Array.ofDim[Double](resultList.size)
    var i = 0
    while(i < resultList.size()){
      val resultItem = resultList.get(i)
      val nameId = resultsString2Id.addString(resultItem.getName, addStringResult).stringId
      resultNameIds(i) = nameId
      similarities(i) = resultItem.getCosineSimilarity()
      i += 1
    }
    directIndexBuilder.addContexts(queryIntegerId, resultNameIds, similarities)
  }

  def build(): FuzzySearchResultsWrapper = {
    new FuzzySearchResultsWrapper(queryString2Id, resultsString2Id, directIndexBuilder.build())
  }

}
