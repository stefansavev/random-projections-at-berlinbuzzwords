package com.stefansavev.fuzzysearchtest;

import com.stefansavev.fuzzysearch.*;

import java.io.*;
import java.util.Iterator;
import java.util.List;

public class MnistTest {

    static FuzzySearchItem parseItem(int lineNumber, String line, int numDimensions){
        String[] tokens = line.split(",");
        int label = Integer.parseInt(tokens[0]);
        double[] values = new double[numDimensions];
        for(int i = 0; i < numDimensions; i ++){
            values[i] = Double.parseDouble(tokens[i + 1]);
        }
        return new FuzzySearchItem(Integer.toString(lineNumber), label, values);
    }

    static void buildIndex(String inputFile, String outputIndexFile) throws IOException {
        int dataDimension = 100;
        int numTrees = 10;
        //create an indexer
        FuzzySearchIndexBuilder indexBuilder = new FuzzySearchIndexBuilder(dataDimension, FuzzySearchEngines.fastTrees(numTrees));

        //read the data points from a file and add them to the indexer one by one
        //each point has a name(string), label(int), and a vector
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        reader.readLine(); //skip header, in this case the format is label,f1,f2,...,f100
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

    static void runQueriesFromFile(String queriesFile, String indexFile) throws IOException {
        FuzzySearchIndex index = FuzzySearchIndex.open(indexFile);
        int dataDimension = index.getDimension();

        String line = null;
        BufferedReader reader = new BufferedReader(new FileReader(queriesFile));
        reader.readLine(); //skip header, in this case the format is label,f1,f2,...,f100
        int lineNumber = 1;

        while ((line = reader.readLine()) != null) {
            FuzzySearchItem item = parseItem(lineNumber, line, dataDimension);
            List<FuzzySearchResult> results = index.getNearestNeighborsByQuery(10, item.getVector());

        }
        reader.close();
    }


    static void runQueriesFromIndex(String indexFile) throws IOException {
        FuzzySearchIndex index = FuzzySearchIndex.open(indexFile);

        Iterator<FuzzySearchItem> itemsIterator = index.getItems();
        double agree = 0.0;
        double disagree = 0.0;
        long start = System.currentTimeMillis();

        while (itemsIterator.hasNext()) {
            FuzzySearchItem item = itemsIterator.next();
            List<FuzzySearchResult> results = index.getNearestNeighborsByQuery(10, item.getVector());
            if (results.get(0).getLabel() != item.getLabel()){
                throw new IllegalStateException("The top result should be the query itself");
            }
            //System.out.println(results.get(1).getCosineSimilarity());
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
        String indexFile = "C:/tmp/output-index/";

        buildIndex(inputTextFile, indexFile);
        //runQueriesFromFile(queriesFile, indexFile);
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
