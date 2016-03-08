package com.stefansavev.fuzzysearchtest;

import com.stefansavev.similaritysearch.*;
import com.stefansavev.similaritysearch.implementation.VectorTypes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class WikipediaLSI {
    static SimilaritySearchItem parseItem(int lineNumber, String line, int numDimensions){
        String[] parts = line.split("\\s+");
        //if (parts.length != (numDimensions)){
        //    throw new IllegalStateException("Invalid data format. Expecting data of dimension " + numDimensions  + " such as: 'the 0.020341 0.011216 0.099383 0.102027 0.041391 -0.010218 ");
        //}
        double[] vec = new double[numDimensions];
        for(int i = 0; i < numDimensions; i ++){
            vec[i] =  Double.parseDouble(parts[i]);
        }

        return new SimilaritySearchItem(Integer.toString(lineNumber), -1, vec); //ignore the label
    }

    static void buildIndex(String inputFile, String outputIndexFile) throws IOException, InvalidDataPointException {
        int dataDimension = 128;
        int numTrees = 50;
        //create an indexer

        SimilaritySearchIndexBuilder indexBuilder = new SimilaritySearchIndexBuilder(outputIndexFile,
                VectorTypes.uncorrelatedFeatures(dataDimension, VectorType.StorageSize.SingleByte),
                SimilarityIndexingEngines.fastTrees(numTrees),
                QueryTypes.cosineSimilarity());

        /*
        FuzzySearchIndexBuilder indexBuilder = new FuzzySearchIndexBuilder(outputIndexFile, dataDimension,
                FuzzySearchEngines.bruteForce (FuzzySearchEngines.FuzzyIndexValueSize.SingleByte));
        */
        //read the data points from a file and add them to the indexer one by one
        //each point has a name(string), label(int), and a vector

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), java.nio.charset.Charset.forName("UTF-8")));
        int lineNumber = 1;
        String line = null;
        while ((line = reader.readLine()) != null) {
            SimilaritySearchItem item = parseItem(lineNumber, line, dataDimension);
            if (lineNumber <= 1000000) {
                indexBuilder.addItem(item);
            }
            else{
                break;
            }
            if (lineNumber % 1000 == 0){
                System.out.println(" " + lineNumber + " docs. read");
            }
            lineNumber ++;
        }
        System.out.println("#docs: " + lineNumber);
        reader.close();

        //build the index
        indexBuilder.build();
    }

    static void runQueriesFromIndex(String indexFile) throws IOException, InvalidDataPointException {
        SimilaritySearchIndex index = SimilaritySearchIndex.open(indexFile);
        SimilaritySearchResultBuilder resultBuilder = new SimilaritySearchResultBuilder();
        Iterator<SimilaritySearchItem> itemsIterator = index.getItems();
        long start = System.currentTimeMillis();

        int numQueries = 0;

        while (itemsIterator.hasNext()) {
            SimilaritySearchItem item = itemsIterator.next();
            List<SimilaritySearchResult> results = index.search(10, item.getVector());
            resultBuilder.addResult(item.getName(), results);
            if (!results.get(0).getName().equals(item.getName())){
                throw new IllegalStateException("The top result should be the query itself");
            }
            if ((++numQueries) % 1000 == 0){
                long endInterm = System.currentTimeMillis();
                double queriesPerSec = 1000.0*numQueries/((double)(endInterm - start + 1L));
                System.out.println("Ran " + numQueries + " queries; Queries per sec. so far.: "  + queriesPerSec);
            }
        }
        /*
        Iterator<FuzzySearchQueryResults> resultsIterator = resultBuilder.build().getIterator();
        while(resultsIterator.hasNext()){
            FuzzySearchQueryResults queryResults = resultsIterator.next();
            System.out.println("******************");
            System.out.println(queryResults.getName());
            for(FuzzySearchResult result: queryResults.getQueryResults()){
                System.out.println(" " + result.getName() + " " + result.getCosineSimilarity());
            }
        }
        */
        //resultBuilder.build()
        SimilaritySearchResults retrieved = resultBuilder.build();
        /*
        retrieved.toTextFile("C:/tmp/output-word-vec-results.txt");
        FuzzySearchResults groundTruth = FuzzySearchResults.fromTextFile("C:/tmp/word-vec-truth.txt");
        RecallEvaluator.evaluateRecall(11, retrieved, groundTruth).printRecalls();
        */

        long end = System.currentTimeMillis();
        double timePerQuery = ((double)(end - start))/numQueries;
        double queriesPerSec = 1000.0*numQueries/((double)(end - start + 1L));

        System.out.println("Total search time in secs.: " + (((double)(end - start))/1000.0));
        System.out.println("Num queries: " + numQueries);
        System.out.println("Time per query in ms.: " + timePerQuery);
        System.out.println("Queries per sec.: " + queriesPerSec);
        //resultBuilder.build().toTextFile("C:/tmp/word-vec-results.txt");
    }

    public static void main(String[] args) throws Exception {
        String inputTextFile = "C:\\wikipedia-parsed\\lsi\\extracted\\wikipedia_lsi128.txt";
        String indexFile = "C:/tmp/output-index-wikipedia-lsi-t1/";

        buildIndex(inputTextFile, indexFile);
        //runQueriesFromIndex(indexFile);
        System.out.println("Free memory: " + Runtime.getRuntime().freeMemory()/(1024));
        SimilaritySearchEvaluationUtils.compareWithBruteForce(indexFile, new Random(481868), 100, 50);
    }


}