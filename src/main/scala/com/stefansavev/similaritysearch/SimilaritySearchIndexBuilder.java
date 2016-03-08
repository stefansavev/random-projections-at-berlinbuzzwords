package com.stefansavev.similaritysearch;

import com.stefansavev.similaritysearch.implementation.FuzzySearchIndexBuilderWrapper;
import com.stefansavev.similaritysearch.implementation.FuzzySearchIndexWrapper;

public class SimilaritySearchIndexBuilder {
    FuzzySearchIndexBuilderWrapper wrapper;

    int dimension;

    public SimilaritySearchIndexBuilder(String backingFile,
                                        VectorType vectorType,
                                        SimilarityIndexingEngine engine,
                                        QueryType queryType) {
        dimension = vectorType.getDimension();
        VectorType.StorageSize storageSize = vectorType.getStorageSize();
        if (engine instanceof SimilarityIndexingEngine.FastTrees) {
            SimilarityIndexingEngine.FastTrees fastTrees = ((SimilarityIndexingEngine.FastTrees) engine);
            wrapper = new FuzzySearchIndexBuilderWrapper(backingFile, dimension, fastTrees.getNumTrees(), storageSize);
        } else {
            //TODO: for the moment use numTrees == 0 as a brute force flag
            SimilarityIndexingEngine.BruteForce bruteForce = ((SimilarityIndexingEngine.BruteForce) engine);
            wrapper = new FuzzySearchIndexBuilderWrapper(backingFile, dimension, 0, storageSize);
        }
    }

    public void addItem(SimilaritySearchItem item) throws InvalidDataPointException {
        if (item == null) {
            throw new IllegalStateException("FuzzySearchItem cannot be null");
        }
        addItem(item.getName(), item.getLabel(), item.getVector());
    }

    public void addItem(String name, int label, double[] dataPoint) throws InvalidDataPointException {
        if (dataPoint.length != dimension) {
            throw new InvalidDataPointException("The dimension of the data point is incorrect");
        }
        if (name == null) {
            throw new IllegalStateException("FuzzySearchItem.name cannot be null");
        }
        double[] normalizedPoint = DataPointVerifier.normalizeDataPointOrFail(dataPoint);
        wrapper.addItem(name, label, normalizedPoint);
    }

    public void build() {
        FuzzySearchIndexWrapper wrappedIndex = wrapper.build();
        //use FuzzySearchIndex.open to open the index
    }
}
