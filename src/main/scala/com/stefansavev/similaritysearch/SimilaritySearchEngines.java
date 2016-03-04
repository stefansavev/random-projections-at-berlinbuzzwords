package com.stefansavev.similaritysearch;

public class SimilaritySearchEngines {
    public enum StorageSize {
        Double,
        Float,
        TwoBytes,
        SingleByte
    }

    public static SimilaritySearchEngine fastTrees(int numTrees, StorageSize valueSize){
        return new SimilaritySearchEngine.FastTrees(numTrees, valueSize);
    }

    public static SimilaritySearchEngine bruteForce(StorageSize valueSize){
        return new SimilaritySearchEngine.BruteForce(valueSize);
    }
}
