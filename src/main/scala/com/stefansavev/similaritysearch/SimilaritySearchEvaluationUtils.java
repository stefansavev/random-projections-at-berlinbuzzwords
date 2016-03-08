package com.stefansavev.similaritysearch;

import com.stefansavev.randomprojections.evaluation.RecallEvaluator;
import com.stefansavev.similaritysearch.implementation.FuzzySearchEvaluationUtilsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class SimilaritySearchEvaluationUtils {
    static Logger logger = LoggerFactory.getLogger(SimilaritySearchEvaluationUtils.class);

    public static SimilaritySearchResults generateRandomTestSet(Random rnd, int numQueries, SimilaritySearchIndex index) {
        return FuzzySearchEvaluationUtilsWrapper.generateRandomTestSet(rnd, numQueries, index);
    }

    public static class TimedResultsOnTestSet {

    }

    public static SimilaritySearchResults resultsOnTestSet(SimilaritySearchIndex index, SimilaritySearchResults testSet, int numResults, boolean bruteForce) throws IOException {

        //FuzzySearchResults testSet = FuzzySearchEvaluationUtils.generateRandomTestSet(rnd, numQueries, numResults, index);

        Iterator<SimilaritySearchQueryResults> queryIterator = testSet.getIterator();

        long start = System.currentTimeMillis();

        SimilaritySearchResultBuilder resultsBuilder = new SimilaritySearchResultBuilder();
        int numProcessedQueries = 0;
        while (queryIterator.hasNext()) {
            String queryId = queryIterator.next().getName();
            //System.out.println("Running query " + queryId + " " + numProcessedQueries + "/" + testSet.getNumberOfQueries());
            double[] queryVector = index.getItemByName(queryId).getVector();
            List<SimilaritySearchResult> results;
            try {
                if (bruteForce) {
                    results = index.bruteForceSearch(numResults, queryVector);
                } else {
                    results = index.search(numResults, queryVector);
                }
            }
            catch(InvalidDataPointException e){
                logger.error("Cannot process the query due to invalid data", e);
                results = new ArrayList<>();
            }
            resultsBuilder.addResult(queryId, results);
            numProcessedQueries++;
        }

        long end = System.currentTimeMillis();
        double timePerQuery = ((double) (end - start)) / numProcessedQueries;
        double queriesPerSec = 1000.0 * numProcessedQueries / ((double) (end - start + 1L));

        System.out.println("Total search time in secs.: " + (((double) (end - start)) / 1000.0));
        System.out.println("Num queries: " + numProcessedQueries);
        System.out.println("Time per query in ms.: " + timePerQuery);
        System.out.println("Queries per sec.: " + queriesPerSec);
        return resultsBuilder.build();
        //RecallEvaluator.evaluateRecall(11, retrieved, testSet).printRecalls();
    }

    public static void compareWithBruteForce(String indexFile, Random rnd, int numQueries, int numResults) throws IOException {
        SimilaritySearchIndex index = SimilaritySearchIndex.open(indexFile);
        SimilaritySearchResults testSet = generateRandomTestSet(rnd, numQueries, index);
        SimilaritySearchResults testSet1 = generateRandomTestSet(rnd, numQueries, index);
        System.out.println("loaded dataset");
        resultsOnTestSet(index, testSet, numResults, false); //with the system
        resultsOnTestSet(index, testSet1, numResults, false); //with the system
        resultsOnTestSet(index, testSet, numResults, false); //with the system
        resultsOnTestSet(index, testSet1, numResults, false); //with the system

        SimilaritySearchResults retrieved = resultsOnTestSet(index, testSet, numResults, false); //with the system

        SimilaritySearchResults expected = resultsOnTestSet(index, testSet, numResults, true); //brute force
        RecallEvaluator.evaluateRecall(11, retrieved, expected).printRecalls();
    }
}
