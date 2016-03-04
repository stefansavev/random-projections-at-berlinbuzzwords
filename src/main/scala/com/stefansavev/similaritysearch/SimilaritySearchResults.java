package com.stefansavev.similaritysearch;

import com.stefansavev.similaritysearch.implementation.FuzzySearchResultsWrapper;

import java.util.Iterator;

public class SimilaritySearchResults {
    FuzzySearchResultsWrapper wrapper;

    public Iterator<SimilaritySearchQueryResults> getIterator(){
        return wrapper.getIterator();
    }

    public int getNumberOfQueries(){
        return wrapper.getNumberOfQueries();
    }

    public SimilaritySearchQueryResults getQueryAtPos(int pos){
        return wrapper.getQueryAtPos(pos);
    }

    public boolean hasQuery(String name){
        return wrapper.hasQuery(name);
    }

    public SimilaritySearchQueryResults getQueryByName(String name){
        return wrapper.getQueryByName(name);
    }

    public void toTextFile(String fileName){
        wrapper.toTextFile(fileName);
    }

    public static SimilaritySearchResults fromTextFile(String fileName){
        return new SimilaritySearchResults(FuzzySearchResultsWrapper.fromTextFile(fileName));
    }

    public SimilaritySearchResults(FuzzySearchResultsWrapper wrapper){
        this.wrapper = wrapper;
    }
}
