package com.stefansavev.similaritysearch.implementation

import java.util
import java.util.Random

import com.stefansavev.randomprojections.utils.RandomUtils
import com.stefansavev.similaritysearch.{SimilaritySearchIndex, SimilaritySearchResult, SimilaritySearchResultBuilder, SimilaritySearchResults}

object FuzzySearchEvaluationUtilsWrapper {

  def generateRandomTestSet(rnd: Random, numQueries: Int, index: SimilaritySearchIndex): SimilaritySearchResults = {
    import scala.collection.JavaConversions._
    val itemNames = index.getItems.toIterator.map(_.getName).toArray

    val sampleIds = RandomUtils.sample(rnd, numQueries, Array.range(0, itemNames.length))
    val builder = new SimilaritySearchResultBuilder()
    for (id <- sampleIds) {
      val queryId = itemNames(id)
      val queryVector = index.getItemByName(queryId).getVector
      val queryResults = new util.ArrayList[SimilaritySearchResult]()
      builder.addResult(queryId, queryResults)
    }
    return builder.build()
  }

}
