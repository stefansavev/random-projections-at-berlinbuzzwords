package com.stefansavev.similaritysearch;

public interface SimilarityIndexingEngine {
    class FastTrees implements SimilarityIndexingEngine {
        private int numTrees;

        public FastTrees(int numTrees) {
            this.numTrees = numTrees;
        }

        public int getNumTrees() {
            return numTrees;
        }

    }

    class BruteForce implements SimilarityIndexingEngine {
        public BruteForce() {
        }
    }
}
