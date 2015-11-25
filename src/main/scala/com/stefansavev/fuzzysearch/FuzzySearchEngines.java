package com.stefansavev.fuzzysearch;

public class FuzzySearchEngines {
    public static enum FuzzyIndexValueSize{
        AsDouble,
        AsFloat,
        As2Byte,
        AsSingleByte
    }

    public static FuzzySearchEngine fastTrees(int numTrees, FuzzyIndexValueSize valueSize){
        return new FuzzySearchEngine.FastTrees(numTrees, valueSize);
    }

    public static FuzzySearchEngine bruteForce(FuzzyIndexValueSize valueSize){
        return new FuzzySearchEngine.BruteForce(valueSize);
    }
}
