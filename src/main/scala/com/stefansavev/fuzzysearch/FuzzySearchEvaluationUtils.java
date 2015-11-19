package com.stefansavev.fuzzysearch;

import com.stefansavev.fuzzysearch.implementation.FuzzySearchEvaluationUtilsWrapper;
import com.stefansavev.randomprojections.evaluation.RecallEvaluator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class FuzzySearchEvaluationUtils {
    public static FuzzySearchResults generateRandomTestSet(Random rnd, int numQueries, FuzzySearchIndex index){
        return FuzzySearchEvaluationUtilsWrapper.generateRandomTestSet(rnd, numQueries, index);
    }

    public static class TimedResultsOnTestSet{

    }

    public static FuzzySearchResults resultsOnTestSet(FuzzySearchIndex index, FuzzySearchResults testSet, int numResults, boolean bruteForce) throws IOException {

        //FuzzySearchResults testSet = FuzzySearchEvaluationUtils.generateRandomTestSet(rnd, numQueries, numResults, index);

        Iterator<FuzzySearchQueryResults> queryIterator = testSet.getIterator();

        long start = System.currentTimeMillis();

        FuzzySearchResultBuilder resultsBuilder = new FuzzySearchResultBuilder();
        int numProcessedQueries = 0;
        while (queryIterator.hasNext()) {
            String queryId = queryIterator.next().getName();
            double[] queryVector = index.getItemByName(queryId).getVector();
            List<FuzzySearchResult> results;
            if ( bruteForce) {
                results = index.bruteForceSearch(numResults, queryVector);
            }
            else{
                results = index.search(numResults, queryVector);
            }
            if (!results.get(0).getName().equals(queryId)){
                throw new IllegalStateException("The top result should be the query itself");
            }
            resultsBuilder.addResult(queryId, results);
            numProcessedQueries ++;
        }

        long end = System.currentTimeMillis();
        double timePerQuery = ((double)(end - start))/numProcessedQueries;
        double queriesPerSec = 1000.0*numProcessedQueries/((double)(end - start + 1L));

        System.out.println("Total search time in secs.: " + (((double)(end - start))/1000.0));
        System.out.println("Num queries: " + numProcessedQueries);
        System.out.println("Time per query in ms.: " + timePerQuery);
        System.out.println("Queries per sec.: " + queriesPerSec);
        return resultsBuilder.build();
        //RecallEvaluator.evaluateRecall(11, retrieved, testSet).printRecalls();
    }

    public static void compareWithBruteForce(String indexFile, Random rnd, int numQueries, int numResults) throws IOException {
        FuzzySearchIndex index = FuzzySearchIndex.open(indexFile);
        FuzzySearchResults testSet = generateRandomTestSet(rnd, numQueries, index);
        FuzzySearchResults retrieved = resultsOnTestSet(index, testSet, numResults, false); //with the system
        FuzzySearchResults expected = resultsOnTestSet(index, testSet, numResults, true); //brute force
        RecallEvaluator.evaluateRecall(11, retrieved, expected).printRecalls();
    }
}
