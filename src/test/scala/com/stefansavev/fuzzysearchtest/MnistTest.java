package com.stefansavev.fuzzysearchtest;

import com.stefansavev.similaritysearch.*;
import com.stefansavev.randomprojections.actors.Application;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class MnistTest {

    static SimilaritySearchItem parseItem(int lineNumber, String line, int numDimensions){
        String[] tokens = line.split(",");
        int label = Integer.parseInt(tokens[0]);
        double[] values = new double[numDimensions];
        for(int i = 0; i < numDimensions; i ++){
            values[i] = Double.parseDouble(tokens[i + 1]);
        }
        String lineNumberStr = Integer.toString(lineNumber);
        String name = lineNumberStr + "#" + lineNumberStr;
        return new SimilaritySearchItem(name, label, values);
    }

    static void buildIndex(String inputFile, String outputIndexFile) throws IOException {
        int dataDimension = 100;
        int numTrees = 10;
        //create an indexer
        SimilaritySearchIndexBuilder indexBuilder = new SimilaritySearchIndexBuilder(outputIndexFile, dataDimension,
                SimilaritySearchEngines.fastTrees(numTrees, SimilaritySearchEngines.StorageSize.Double));

        //FuzzySearchIndexBuilder indexBuilder = new FuzzySearchIndexBuilder(dataDimension,
        //        FuzzySearchEngines.bruteForce(FuzzySearchEngines.FuzzyIndexValueSize.SingleByte));

        //read the data points from a file and add them to the indexer one by one
        //each point has a name(string), label(int), and a vector
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        reader.readLine(); //skip header, in this case the format is label,f1,f2,...,f100
        int lineNumber = 1;
        String line = null;
        while ((line = reader.readLine()) != null) {
            SimilaritySearchItem item = parseItem(lineNumber, line, dataDimension);
            indexBuilder.addItem(item);
            lineNumber ++;
        }
        reader.close();

        //build the index
        indexBuilder.build();
    }

    static void runQueriesFromFile(String queriesFile, String indexFile) throws IOException {
        SimilaritySearchIndex index = SimilaritySearchIndex.open(indexFile);
        int dataDimension = index.getDimension();

        String line = null;
        BufferedReader reader = new BufferedReader(new FileReader(queriesFile));
        reader.readLine(); //skip header, in this case the format is label,f1,f2,...,f100
        int lineNumber = 1;

        while ((line = reader.readLine()) != null) {
            SimilaritySearchItem item = parseItem(lineNumber, line, dataDimension);
            List<SimilaritySearchResult> results = index.search(10, item.getVector());
        }
        reader.close();
    }

    /*
    static void compareWithBruteForce(String indexFile, Random rnd, int numQueries, int numResults) throws IOException {

        FuzzySearchIndex index = FuzzySearchIndex.open(indexFile);
        long start = System.currentTimeMillis();
        FuzzySearchResults testSet = FuzzySearchEvaluationUtils.generateRandomTestSet(rnd, numQueries, numResults, index);
        long end = System.currentTimeMillis();
        double queriesPerSecBruteForce = 1000.0*testSet.getNumberOfQueries()/((double)(end - start + 1L));

        Iterator<FuzzySearchQueryResults> queryIterator = testSet.getIterator();

        start = System.currentTimeMillis();

        FuzzySearchResultBuilder resultsBuilder = new FuzzySearchResultBuilder();
        int numProcessedQueries = 0;
        while (queryIterator.hasNext()) {
            String queryId = queryIterator.next().getName();
            double[] queryVector = index.getItemByName(queryId).getVector();
            List<FuzzySearchResult> results = index.search(10, queryVector);
            if (!results.get(0).getName().equals(queryId)){
                throw new IllegalStateException("The top result should be the query itself");
            }
            resultsBuilder.addResult(queryId, results);
            numProcessedQueries ++;
        }

        end = System.currentTimeMillis();
        double timePerQuery = ((double)(end - start))/numQueries;
        double queriesPerSec = 1000.0*numProcessedQueries/((double)(end - start + 1L));

        System.out.println("Total search time in secs.: " + (((double)(end - start))/1000.0));
        System.out.println("Num queries: " + numQueries);
        System.out.println("Time per query in ms.: " + timePerQuery);
        System.out.println("Queries per sec.: " + queriesPerSec);
        System.out.println("Queries per sec BRUTE FORCE.: " + queriesPerSecBruteForce);
        FuzzySearchResults retrieved = resultsBuilder.build();

        RecallEvaluator.evaluateRecall(11, retrieved, testSet).printRecalls();
    }
    */
    static void runQueriesFromIndex(String indexFile) throws IOException {
        SimilaritySearchIndex index = SimilaritySearchIndex.open(indexFile);

        Iterator<SimilaritySearchItem> itemsIterator = index.getItems();
        double agree = 0.0;
        double disagree = 0.0;
        long start = System.currentTimeMillis();

        while (itemsIterator.hasNext()) {
            SimilaritySearchItem item = itemsIterator.next();
            List<SimilaritySearchResult> results = index.search(10, item.getVector());
            if (!results.get(0).getName().equals(item.getName())){
                throw new IllegalStateException("The top result should be the query itself");
            }
            if (item.getLabel() == results.get(1).getLabel()){
                agree ++;
            }
            else{
                disagree ++;
            }
        }
        long end = System.currentTimeMillis();
        double accuracy = 100.0*agree/(agree + disagree);
        int numQueries = (int)(agree + disagree);
        double timePerQuery = ((double)(end - start))/numQueries;
        double queriesPerSec = 1000.0*numQueries/((double)(end - start + 1L));

        System.out.println("Total search time in secs.: " + (((double)(end - start))/1000.0));
        System.out.println("Num queries: " + numQueries);
        System.out.println("Accuracy estimate: " + accuracy);
        System.out.println("Time per query in ms.: " + timePerQuery);
        System.out.println("Queries per sec.: " + queriesPerSec);
    }

    public static void main(String[] args) throws Exception {
        String inputTextFile = "D:/RandomTreesData-144818512896186816/input/" + "mnist/svdpreprocessed/train.csv";
        String queriesFile = inputTextFile; //same as training in this example
        String indexFile = "C:/tmp/output-index-mnist-2/";

        /*
        String inputTextFile = "C:/tmp/fuzzysearch-demo/testdata/mnist/preprocessed-train.csv";
        String queriesFile = inputTextFile; //same as training in this example
        String indexFile = "C:/tmp/output-index-mnist-1/";
        */
        buildIndex(inputTextFile, indexFile);
        //runQueriesFromFile(queriesFile, indexFile);
        //runQueriesFromIndex(indexFile);
        Runtime runtime = Runtime.getRuntime();
        int mb = 1024*1024;
        //Print used memory
        System.out.println("Before index load: Used Memory [in MB]:"
                + (runtime.totalMemory() - runtime.freeMemory()) / mb);

        SimilaritySearchIndex index = SimilaritySearchIndex.open(indexFile);
        System.out.println("After index load: Used Memory [in MB]:"
                + (runtime.totalMemory() - runtime.freeMemory()) / mb);


        //FuzzySearchResults testSet = FuzzySearchEvaluationUtils.generateRandomTestSet(new Random(481868), 1000, index);
        //
        //FuzzySearchResults expected = FuzzySearchEvaluationUtils.resultsOnTestSet(index, testSet, 50, true); //brute force
        //expected.toTextFile("C:/tmp/mnist-testset.txt");
        //FuzzySearchResults expected = FuzzySearchResults.fromTextFile("C:/tmp/mnist-testset.txt");
        //FuzzySearchResults retrieved =  FuzzySearchEvaluationUtils.resultsOnTestSet(index, expected, 1000, false); //with the system
        //RecallEvaluator.evaluateRecall(11, retrieved, expected).printRecalls();
        SimilaritySearchEvaluationUtils.compareWithBruteForce(indexFile, new Random(481868), 1000, 50);
        Application.shutdown();
        //
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
