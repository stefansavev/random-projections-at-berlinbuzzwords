package com.stefansavev.fuzzysearchtest;

import com.stefansavev.similaritysearch.*;
import com.stefansavev.randomprojections.actors.Application;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class Cifar100Parts {
    static SimilaritySearchItem parseItem(int lineNumber, String line, int numDimensions){
        String[] parts = line.split("\\s+");
        //System.out.println("Parts len: " + parts.length);
        if (parts.length != 67){
            return null;
        }

        if (parts.length != (3 + numDimensions)){
            throw new IllegalStateException("Invalid data format. Expecting data of dimension " + numDimensions  + line);
        }
        String fileIndex = parts[0];
        String partId = parts[1];
        int labelId = Integer.parseInt(parts[2]);

        double[] vec = new double[numDimensions];
        for(int i = 0; i < numDimensions; i ++){
            vec[i] =  Double.parseDouble(parts[i + 1]);
        }
        return new SimilaritySearchItem(fileIndex + "_" + partId, labelId, vec);

    }

    static void buildIndex(String inputFile, String outputIndexFile) throws IOException {
        int dataDimension = 64;
        int numTrees = 50; //150;
        //create an indexer
        SimilaritySearchIndexBuilder indexBuilder = new SimilaritySearchIndexBuilder(outputIndexFile, dataDimension,
                SimilaritySearchEngines.fastTrees(numTrees, SimilaritySearchEngines.FuzzyIndexValueSize.As2Byte));

        //read the data points from a file and add them to the indexer one by one
        //each point has a name(string), label(int), and a vector

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), java.nio.charset.Charset.forName("UTF-8")));
        int lineNumber = 1;
        String line = null;
        while ((line = reader.readLine()) != null) {
            SimilaritySearchItem item = parseItem(lineNumber, line, dataDimension);
            if (item != null) {
                indexBuilder.addItem(item);
            }
            lineNumber ++;
        }
        reader.close();
        //build the index
        indexBuilder.build();
    }

    static void runQueriesFromIndex(String indexFile) throws IOException {
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
        String inputTextFile = "/home/stefan2/data/cifar/cifar_parts.csv";
        String indexFile = "/tmp/index-cifar";

        //buildIndex(inputTextFile, indexFile);
        //runQueriesFromIndex(indexFile);
        SimilaritySearchEvaluationUtils.compareWithBruteForce(indexFile, new Random(481868), 100, 50);

        Application.shutdown();
    }
}
