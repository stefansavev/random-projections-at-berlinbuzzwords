package com.stefansavev.fuzzysearch;

import com.stefansavev.fuzzysearch.implementation.FuzzySearchIndexBuilderWrapper;
import com.stefansavev.fuzzysearch.implementation.FuzzySearchIndexWrapper;

public class FuzzySearchIndexBuilder {
    FuzzySearchIndexBuilderWrapper wrapper;
    int dimension;

    public FuzzySearchIndexBuilder(int dimension, FuzzySearchEngine engine){
        this.dimension = dimension;
        wrapper = new FuzzySearchIndexBuilderWrapper(dimension, ((FuzzySearchEngine.FastTrees)engine).numTrees);
    }

    public void addItem(FuzzySearchItem item){
        addItem(item.getName(), item.getLabel(), item.getVector());
    }

    public void addItem(String name, int label, double[] dataPoint) throws InvalidDataPointException{
        if (dataPoint.length != dimension){
            throw new InvalidDataPointException("The dimension of the data point is incorrect");
        }
        double[] normalizedPoint = DataPointVerifier.normalizeDataPointOrFail(dataPoint);
        wrapper.addItem(name, label, normalizedPoint);
    }

    public FuzzySearchIndex build(){
        FuzzySearchIndexWrapper wrappedIndex =  wrapper.build();
        return new FuzzySearchIndex(wrappedIndex);
    }
}
