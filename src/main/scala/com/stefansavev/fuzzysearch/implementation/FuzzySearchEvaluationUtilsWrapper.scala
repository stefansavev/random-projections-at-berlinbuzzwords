package com.stefansavev.fuzzysearch.implementation

import java.util
import java.util.Random

import com.stefansavev.fuzzysearch.{FuzzySearchResult, FuzzySearchResultBuilder, FuzzySearchIndex, FuzzySearchResults}
import com.stefansavev.randomprojections.utils.RandomUtils

object FuzzySearchEvaluationUtilsWrapper {

  def generateRandomTestSet(rnd: Random, numQueries: Int, index: FuzzySearchIndex): FuzzySearchResults = {
    import scala.collection.JavaConversions._
    val itemNames = index.getItems.toIterator.map(_.getName).toArray

    val sampleIds = RandomUtils.sample(rnd, numQueries, Array.range(0, itemNames.length))
    val builder = new FuzzySearchResultBuilder()
    for(id <- sampleIds){
      val queryId = itemNames(id)
      val queryVector = index.getItemByName(queryId).getVector
      val queryResults = new util.ArrayList[FuzzySearchResult]()
      builder.addResult(queryId, queryResults)
    }
    return builder.build()
  }

}
