package com.stefansavev.fuzzysearch;

public class FuzzySearchEngines {
    public static FuzzySearchEngine fastTrees(int numTrees){
        return new FuzzySearchEngine.FastTrees(numTrees);
     }
}
