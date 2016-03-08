package com.stefansavev.similaritysearch;

import com.stefansavev.similaritysearch.implementation.FuzzySearchIndexWrapper;

import java.util.Iterator;
import java.util.List;

public class SimilaritySearchIndex {
    FuzzySearchIndexWrapper wrapper;

    public SimilaritySearchIndex(FuzzySearchIndexWrapper wrapper) {
        this.wrapper = wrapper;
    }

    public static SimilaritySearchIndex open(String fileName) {
        return new SimilaritySearchIndex(FuzzySearchIndexWrapper.open(fileName));
    }

    public List<SimilaritySearchResult> search(int numNeighbors, double[] query) throws InvalidDataPointException {
        double[] normalizedQuery = DataPointVerifier.normalizeDataPointOrFail(query);
        if (wrapper.searcherSettings().randomTrees().trees().length == 0) {
            //it's brute force (work around)
            return wrapper.bruteForceSearch(numNeighbors, normalizedQuery);
        }
        return wrapper.getNearestNeighborsByQuery(numNeighbors, normalizedQuery);
    }

    //this will go away
    public List<SimilaritySearchResult> bruteForceSearch(int numNeighbors, double[] query) throws InvalidDataPointException {
        double[] normalizedQuery = DataPointVerifier.normalizeDataPointOrFail(query);
        return wrapper.bruteForceSearch(numNeighbors, normalizedQuery);
    }

    public Iterator<SimilaritySearchItem> getItems() {
        return wrapper.getItems();
    }

    public SimilaritySearchItem getItemByName(String name) {
        return wrapper.getItemByName(name);
    }

    public int getDimension() {
        return wrapper.getDimension();
    }
}

