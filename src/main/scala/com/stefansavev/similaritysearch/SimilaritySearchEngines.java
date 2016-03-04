package com.stefansavev.similaritysearch;

public class SimilaritySearchEngines {
    public static enum FuzzyIndexValueSize{
        AsDouble,
        AsFloat,
        As2Byte,
        AsSingleByte
    }

    public static SimilaritySearchEngine fastTrees(int numTrees, FuzzyIndexValueSize valueSize){
        return new SimilaritySearchEngine.FastTrees(numTrees, valueSize);
    }

    public static SimilaritySearchEngine bruteForce(FuzzyIndexValueSize valueSize){
        return new SimilaritySearchEngine.BruteForce(valueSize);
    }
}
