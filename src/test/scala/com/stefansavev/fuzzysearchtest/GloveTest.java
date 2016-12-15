package com.stefansavev.fuzzysearchtest;

import com.stefansavev.randomprojections.actors.Application;
import com.stefansavev.similaritysearch.*;
import com.stefansavev.similaritysearch.implementation.VectorTypes;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class GloveTest {
    static SimilaritySearchItem parseItem(int lineNumber, String line, int numDimensions) {
        String[] parts = line.split("\\s+");
        if (parts.length != (1 + numDimensions)) {
            throw new IllegalStateException("Invalid data format. Expecting data of dimension " + numDimensions + " such as: 'the 0.020341 0.011216 0.099383 0.102027 0.041391 -0.010218 ");
        }
        String word = parts[0];
        double[] vec = new double[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            vec[i] = Double.parseDouble(parts[i + 1]);
        }
        return new SimilaritySearchItem(word, -1, vec); //ignore the label
    }

    static void buildIndex(String inputFile, String outputIndexFile, int numTrees) throws IOException, InvalidDataPointException {
        int dataDimension = 100;

        //create an indexer
        SimilaritySearchIndexBuilder indexBuilder = new SimilaritySearchIndexBuilder(outputIndexFile,
                VectorTypes.uncorrelatedFeatures(dataDimension, VectorType.StorageSize.Double),
                SimilarityIndexingEngines.fastTrees(numTrees),
                QueryTypes.cosineSimilarity());

        //read the data points from a file and add them to the indexer one by one
        //each point has a name(string), label(int), and a vector
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), java.nio.charset.Charset.forName("UTF-8")));
        int lineNumber = 1;
        String line = null;
        while ((line = reader.readLine()) != null) {
            SimilaritySearchItem item = parseItem(lineNumber, line, dataDimension);
            indexBuilder.addItem(item);
            lineNumber++;
        }
        reader.close();
        indexBuilder.build();
    }

    static String runQueriesPrintResults(String indexFile, List<String> queries) throws InvalidDataPointException {
        StringBuilder output = new StringBuilder();
        SimilaritySearchIndex index = SimilaritySearchIndex.open(indexFile);

        for (String query : queries) {
            SimilaritySearchItem item = index.getItemByName(query);
            List<SimilaritySearchResult> results = index.search(10, item.getVector());
            output.append("----------------------\n");
            output.append("query: " + item.getName() + "\n");
            for (int i = 0; i < Math.min(10, results.size()); i++) {
                SimilaritySearchResult result = results.get(i);
                String line = item.getName() + ": " + result.getName() + " " + result.getCosineSimilarity() + "\n";
                output.append(line);
            }
        }
        String result = output.toString();
        System.out.println(result);
        return result;
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
            if (!results.get(0).getName().equals(item.getName())) {
                throw new IllegalStateException("The top result should be the query itself");
            }
            if ((++numQueries) % 1000 == 0) {
                long endInterm = System.currentTimeMillis();
                double queriesPerSec = 1000.0 * numQueries / ((double) (endInterm - start + 1L));
                System.out.println("Ran " + numQueries + " queries; Queries per sec. so far.: " + queriesPerSec);
            }
        }
        long end = System.currentTimeMillis();
        double timePerQuery = ((double) (end - start)) / numQueries;
        double queriesPerSec = 1000.0 * numQueries / ((double) (end - start + 1L));

        System.out.println("Total search time in secs.: " + (((double) (end - start)) / 1000.0));
        System.out.println("Num queries: " + numQueries);
        System.out.println("Time per query in ms.: " + timePerQuery);
        System.out.println("Queries per sec.: " + queriesPerSec);
    }

    public static String run(String inputTextFile, String indexFile, int numTrees) throws Exception {
        //download: http://nlp.stanford.edu/data/glove.6B.zip and use the file 100d.txt
        buildIndex(inputTextFile, indexFile, numTrees);
        //runQueriesFromIndex(indexFile);
        return runQueriesPrintResults(indexFile, Arrays.asList("java", "scala", "c++", "berlin", "london", "germany", "microsoft"));
        //SimilaritySearchEvaluationUtils.compareWithBruteForce(indexFile, new Random(481868), 1000, 50);
        //Application.shutdown();
    }

    public static void main(String[] args) throws Exception {
        //download: http://nlp.stanford.edu/data/glove.6B.zip and use the file 100d.txt
        String inputTextFile = "/projects/random-projections-at-berlinbuzzwords/src/test/resources/glove/glove.6B.100d.txt";
        String indexFile = "/tmp/output-index-glove-v1/";
        run(inputTextFile, indexFile, 150);
        Application.shutdown();
    }


}
