package com.stefansavev.fuzzysearch;

import com.stefansavev.fuzzysearch.implementation.FuzzySearchIndexBuilderWrapper;
import com.stefansavev.fuzzysearch.implementation.FuzzySearchIndexWrapper;

public class FuzzySearchIndexBuilder {
    FuzzySearchIndexBuilderWrapper wrapper;
    int dimension;

    public FuzzySearchIndexBuilder(String backingFile, int dimension, FuzzySearchEngine engine){
        this.dimension = dimension;
        if (engine instanceof FuzzySearchEngine.FastTrees){
            FuzzySearchEngine.FastTrees fastTrees = ((FuzzySearchEngine.FastTrees)engine);
            wrapper = new FuzzySearchIndexBuilderWrapper(backingFile, dimension, fastTrees.getNumTrees(), fastTrees.getValueSize());
        }
        else{
            //for the moment use numTrees == 0 as a brute force flag
            FuzzySearchEngine.BruteForce bruteForce = ((FuzzySearchEngine.BruteForce)engine);
            wrapper = new FuzzySearchIndexBuilderWrapper(backingFile, dimension, 0, bruteForce.getValueSize());
        }
    }

    public void addItem(FuzzySearchItem item){
        if (item == null){
            throw new IllegalStateException("FuzzySearchItem cannot be null");
        }
        addItem(item.getName(), item.getLabel(), item.getVector());
    }

    public void addItem(String name, int label, double[] dataPoint) throws InvalidDataPointException{
        if (dataPoint.length != dimension){
            throw new InvalidDataPointException("The dimension of the data point is incorrect");
        }
        if (name == null){
            throw new IllegalStateException("FuzzySearchItem.name cannot be null");
        }
        double[] normalizedPoint = DataPointVerifier.normalizeDataPointOrFail(dataPoint);
        wrapper.addItem(name, label, normalizedPoint);
    }

    public FuzzySearchIndex build(){
        FuzzySearchIndexWrapper wrappedIndex =  wrapper.build();
        return new FuzzySearchIndex(wrappedIndex);
    }
}
