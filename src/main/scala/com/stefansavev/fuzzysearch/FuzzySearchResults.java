package com.stefansavev.fuzzysearch;

import com.stefansavev.fuzzysearch.implementation.FuzzySearchResultsWrapper;

import java.util.Iterator;

public class FuzzySearchResults {
    FuzzySearchResultsWrapper wrapper;

    public Iterator<FuzzySearchQueryResults> getIterator(){
        return wrapper.getIterator();
    }

    public int getNumberOfQueries(){
        return wrapper.getNumberOfQueries();
    }

    public FuzzySearchQueryResults getQueryAtPos(int pos){
        return wrapper.getQueryAtPos(pos);
    }

    public boolean hasQuery(String name){
        return wrapper.hasQuery(name);
    }

    public FuzzySearchQueryResults getQueryByName(String name){
        return wrapper.getQueryByName(name);
    }

    public void toTextFile(String fileName){
        wrapper.toTextFile(fileName);
    }

    public static FuzzySearchResults fromTextFile(String fileName){
        return new FuzzySearchResults(FuzzySearchResultsWrapper.fromTextFile(fileName));
    }

    public FuzzySearchResults(FuzzySearchResultsWrapper wrapper){
        this.wrapper = wrapper;
    }
}
