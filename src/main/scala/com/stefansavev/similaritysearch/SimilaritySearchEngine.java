package com.stefansavev.similaritysearch;

public interface SimilaritySearchEngine {
    public static class FastTrees implements SimilaritySearchEngine {
        private int numTrees;
        private SimilaritySearchEngines.FuzzyIndexValueSize valueSize;

        public FastTrees(int numTrees, SimilaritySearchEngines.FuzzyIndexValueSize valueSize){
            this.numTrees = numTrees;
            this.valueSize = valueSize;
        }

        public int getNumTrees(){
            return numTrees;
        }

        public SimilaritySearchEngines.FuzzyIndexValueSize getValueSize(){
            return valueSize;
        }
    }

    public static class BruteForce implements SimilaritySearchEngine {
        private SimilaritySearchEngines.FuzzyIndexValueSize valueSize;

        public BruteForce(SimilaritySearchEngines.FuzzyIndexValueSize valueSize) {
            this.valueSize = valueSize;
        }

        public SimilaritySearchEngines.FuzzyIndexValueSize getValueSize(){
            return valueSize;
        }
    }
}
