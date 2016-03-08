package com.stefansavev.similaritysearch;

public class SimilarityIndexingEngines {

    public static SimilarityIndexingEngine fastTrees(int numTrees) {
        return new SimilarityIndexingEngine.FastTrees(numTrees);
    }

    public static SimilarityIndexingEngine bruteForce() {
        return new SimilarityIndexingEngine.BruteForce();
    }
}
