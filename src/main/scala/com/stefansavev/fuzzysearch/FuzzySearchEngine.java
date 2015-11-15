package com.stefansavev.fuzzysearch;

public interface FuzzySearchEngine {
    public static class FastTrees implements FuzzySearchEngine{
        public int numTrees;

        public FastTrees(int numTrees){
            this.numTrees = numTrees;
        }
    }
}
