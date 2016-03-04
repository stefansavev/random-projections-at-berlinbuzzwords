package com.stefansavev.similaritysearch;

public interface SimilaritySearchEngine {
    public static class FastTrees implements SimilaritySearchEngine {
        private int numTrees;
        private SimilaritySearchEngines.StorageSize valueSize;

        public FastTrees(int numTrees, SimilaritySearchEngines.StorageSize valueSize){
            this.numTrees = numTrees;
            this.valueSize = valueSize;
        }

        public int getNumTrees(){
            return numTrees;
        }

        public SimilaritySearchEngines.StorageSize getValueSize(){
            return valueSize;
        }
    }

    public static class BruteForce implements SimilaritySearchEngine {
        private SimilaritySearchEngines.StorageSize valueSize;

        public BruteForce(SimilaritySearchEngines.StorageSize valueSize) {
            this.valueSize = valueSize;
        }

        public SimilaritySearchEngines.StorageSize getValueSize(){
            return valueSize;
        }
    }
}
