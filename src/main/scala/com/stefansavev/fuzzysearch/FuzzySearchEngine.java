package com.stefansavev.fuzzysearch;

public interface FuzzySearchEngine {
    public static class FastTrees implements FuzzySearchEngine{
        private int numTrees;
        private FuzzySearchEngines.FuzzyIndexValueSize valueSize;

        public FastTrees(int numTrees, FuzzySearchEngines.FuzzyIndexValueSize valueSize){
            this.numTrees = numTrees;
            this.valueSize = valueSize;
        }

        public int getNumTrees(){
            return numTrees;
        }

        public FuzzySearchEngines.FuzzyIndexValueSize getValueSize(){
            return valueSize;
        }
    }

    public static class BruteForce implements FuzzySearchEngine{
        private FuzzySearchEngines.FuzzyIndexValueSize valueSize;

        public BruteForce(FuzzySearchEngines.FuzzyIndexValueSize valueSize) {
            this.valueSize = valueSize;
        }

        public FuzzySearchEngines.FuzzyIndexValueSize getValueSize(){
            return valueSize;
        }
    }
}
