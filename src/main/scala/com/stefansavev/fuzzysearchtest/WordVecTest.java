package com.stefansavev.fuzzysearchtest;

import com.stefansavev.fuzzysearch.*;
import com.stefansavev.fuzzysearch.FuzzySearchResultBuilder;
import com.stefansavev.randomprojections.evaluation.RecallEvaluator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class WordVecTest {
    static FuzzySearchItem parseItem(int lineNumber, String line, int numDimensions){
        String[] parts = line.split("\\s+");
        if (parts.length != (1 + numDimensions)){
            throw new IllegalStateException("Invalid data format. Expecting data of dimension " + numDimensions  + " such as: 'the 0.020341 0.011216 0.099383 0.102027 0.041391 -0.010218 ");
        }
        String word = parts[0];
        double[] vec = new double[numDimensions];
        for(int i = 0; i < numDimensions; i ++){
            vec[i] =  Double.parseDouble(parts[i + 1]);
        }
        return new FuzzySearchItem(word, -1, vec); //ignore the label
     }

    static void buildIndex(String inputFile, String outputIndexFile) throws IOException {
        int dataDimension = 200;
        int numTrees = 150; //20;
        //create an indexer
        FuzzySearchIndexBuilder indexBuilder = new FuzzySearchIndexBuilder(dataDimension, FuzzySearchEngines.fastTrees(numTrees));

        //read the data points from a file and add them to the indexer one by one
        //each point has a name(string), label(int), and a vector
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        int lineNumber = 1;
        String line = null;
        while ((line = reader.readLine()) != null) {
            FuzzySearchItem item = parseItem(lineNumber, line, dataDimension);
            indexBuilder.addItem(item);
            lineNumber ++;
        }
        reader.close();

        //build the index
        FuzzySearchIndex index = indexBuilder.build();

        //save the index to file
        index.save(outputIndexFile);
    }

    static void runQueriesFromIndex(String indexFile) throws IOException {
        FuzzySearchIndex index = FuzzySearchIndex.open(indexFile);
        FuzzySearchResultBuilder resultBuilder = new FuzzySearchResultBuilder();
        Iterator<FuzzySearchItem> itemsIterator = index.getItems();
        long start = System.currentTimeMillis();

        int numQueries = 0;

        while (itemsIterator.hasNext()) {
            FuzzySearchItem item = itemsIterator.next();
            List<FuzzySearchResult> results = index.search(10, item.getVector());
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
        FuzzySearchResults retrieved = resultBuilder.build();
        retrieved.toTextFile("C:/tmp/output-word-vec-results.txt");
        FuzzySearchResults groundTruth = FuzzySearchResults.fromTextFile("C:/tmp/word-vec-truth.txt");
        RecallEvaluator.evaluateRecall(11, retrieved, groundTruth).printRecalls();


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
        String inputTextFile = "D:/RandomTreesData-144818512896186816/input/" + "wordvec/wordvec.txt";
        String indexFile = "C:/tmp/output-index-wordvec/";

        buildIndex(inputTextFile, indexFile);
        runQueriesFromIndex(indexFile);
    }
    /*
    output:
    DataFrameView(42000, 100)
    Using SVD on the full data
    Nov 15, 2015 7:21:03 PM com.github.fommil.netlib.BLAS <clinit>
    WARNING: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS
    Nov 15, 2015 7:21:04 PM com.github.fommil.jni.JniLoader liberalLoad
    INFO: successfully loaded D:\Users\stefan\AppData\Local\Temp\jniloader8503132299230406528netlib-native_ref-win-x86_64.dll
    Nov 15, 2015 7:21:04 PM com.github.fommil.netlib.LAPACK <clinit>
    WARNING: Failed to load implementation from: com.github.fommil.netlib.NativeSystemLAPACK
    Nov 15, 2015 7:21:04 PM com.github.fommil.jni.JniLoader load
    INFO: already loaded netlib-native_ref-win-x86_64.dll
    Started transformation with SVD
    Processed 0 rows
    Processed 5000 rows
    Processed 10000 rows
    Processed 15000 rows
    Processed 20000 rows
    Processed 25000 rows
    Processed 30000 rows
    Processed 35000 rows
    Processed 40000 rows
    Finished transformation with SVD
    Time for 'Build tree 0' 228 ms.
    Time for 'Build tree 1' 121 ms.
    Time for 'Build tree 2' 129 ms.
    Time for 'Build tree 3' 94 ms.
    Time for 'Build tree 4' 73 ms.
    Time for 'Build tree 5' 66 ms.
    Time for 'Build tree 6' 73 ms.
    Time for 'Build tree 7' 55 ms.
    Time for 'Build tree 8' 78 ms.
    Time for 'Build tree 9' 75 ms.
    Time for 'Create trees' 4.384 secs.
    Total search time in secs.: 6.85
    Num queries: 42000
    Accuracy estimate: 97.46666666666667
    Time per query in ms.: 0.1630952380952381
    Queries per sec.: 6130.491898992847
     */

}
