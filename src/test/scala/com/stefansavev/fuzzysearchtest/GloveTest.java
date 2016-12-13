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

    static void buildIndex(String inputFile, String outputIndexFile) throws IOException, InvalidDataPointException {
        int dataDimension = 100;
        int numTrees = 150;

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

    static void runQueriesPrintResults(String indexFile, List<String> queries) throws InvalidDataPointException {
        SimilaritySearchIndex index = SimilaritySearchIndex.open(indexFile);

        for (String query : queries) {
            SimilaritySearchItem item = index.getItemByName(query);
            List<SimilaritySearchResult> results = index.search(10, item.getVector());
            System.out.println("----------------------");
            System.out.println("query: " + item.getName());
            for (int i = 0; i < Math.min(10, results.size()); i++) {
                SimilaritySearchResult result = results.get(i);
                System.out.println(item.getName() + ": " + result.getName() + " " + result.getCosineSimilarity());
            }
        }
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

    public static void main(String[] args) throws Exception {
        //download: http://nlp.stanford.edu/data/glove.6B.zip and use the file 100d.txt
        String inputTextFile = "/projects/random-projections-at-berlinbuzzwords/src/test/resources/glove/glove.6B.100d.txt";
        String indexFile = "/tmp/output-index-glove-v1/";

        buildIndex(inputTextFile, indexFile);
        //runQueriesFromIndex(indexFile);
        runQueriesPrintResults(indexFile, Arrays.asList("java", "scala", "c++", "berlin", "london", "germany", "microsoft"));
        //SimilaritySearchEvaluationUtils.compareWithBruteForce(indexFile, new Random(481868), 1000, 50);
        Application.shutdown();
        /*Expected output:
        ----------------------
query: java
java: java 1.0
java: sumatra 0.6641601171881357
java: surabaya 0.6600468177066761
java: semarang 0.6302395346623364
java: sulawesi 0.6134430128761578
java: yogyakarta 0.603346025285713
java: bandung 0.6005889678698815
java: mindanao 0.5711929368963106
java: banten 0.5604899935949604
java: borneo 0.5586322711053665
----------------------
query: scala
scala: scala 1.0
scala: teatro 0.6635824384886557
scala: muti 0.6430658040717878
scala: opera 0.6299091977879665
scala: covent 0.5995722534629364
scala: verdi 0.5970718408905353
scala: bolshoi 0.5808316784563217
scala: repertory 0.5741132803269162
scala: milano 0.5604457711481539
scala: philharmonic 0.5587969666797273
----------------------
query: c++
c++: c++ 1.0
c++: compiler 0.7526918763377063
c++: fortran 0.729840646550826
c++: compilers 0.7106519500030678
c++: javascript 0.6958026237810975
c++: objective-c 0.6713192226391669
c++: php 0.6672695402840483
c++: object-oriented 0.6663514906760801
c++: perl 0.6368851967185153
c++: cobol 0.6069628363016288
----------------------
query: berlin
berlin: berlin 1.0
berlin: munich 0.8067099661783561
berlin: vienna 0.794233193381004
berlin: hamburg 0.7488427872032346
berlin: warsaw 0.7330315141670991
berlin: bonn 0.7310878647418511
berlin: germany 0.7289800545613594
berlin: prague 0.7246278118266953
berlin: dresden 0.7223253834042958
berlin: frankfurt 0.7162715946806268
----------------------
query: london
london: london 1.0
london: sydney 0.75360958077475
london: paris 0.7337677235699642
london: melbourne 0.7044002840281416
london: york 0.6945249319623287
london: dublin 0.6912744661716886
london: edinburgh 0.6830862053795652
london: prohertrib 0.67862498210445
london: glasgow 0.6695584219196183
london: british 0.6597145572570191
----------------------
query: germany
germany: germany 1.0
germany: austria 0.8086650136525269
germany: switzerland 0.7793212159027886
germany: german 0.7745245923792147
germany: europe 0.7593437235400956
germany: poland 0.7552422729002328
germany: netherlands 0.7447889863268574
germany: denmark 0.7414340758322
germany: italy 0.7400852563891842
germany: france 0.7294081117117922
----------------------
query: microsoft
microsoft: microsoft 1.0
microsoft: google 0.8104098502002781
microsoft: netscape 0.788514923431569
microsoft: ibm 0.7883655586472389
microsoft: intel 0.7864235983441769
microsoft: software 0.7803657394718166
microsoft: yahoo 0.7737911235365997
microsoft: apple 0.7449405756323191
microsoft: aol 0.7413646896084234
microsoft: compaq 0.7276570202909849
*/
    }


}
