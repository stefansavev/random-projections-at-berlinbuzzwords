package com.stefansavev.fuzzysearch;

import com.stefansavev.fuzzysearch.implementation.FuzzySearchIndexBuilderWrapper;
import com.stefansavev.fuzzysearch.implementation.FuzzySearchResultsBuilderWrapper;
import com.stefansavev.fuzzysearch.implementation.FuzzySearchResultsWrapper;

import java.util.List;

public class FuzzySearchResultBuilder {
    boolean inInsertMode = true;
    FuzzySearchResultsBuilderWrapper wrapper = new FuzzySearchResultsBuilderWrapper();

    public void addResult(String queryId, List<FuzzySearchResult> resultList){
        if (!inInsertMode){
            throw new IllegalStateException("Method build() has already been called.");
        }
        wrapper.addResult(queryId, resultList);
    }

    public FuzzySearchResults build(){
        if (!inInsertMode){
            throw new IllegalStateException("Method build() has already been called.");
        }
        inInsertMode = false;
        FuzzySearchResultsWrapper searchResultsWrapper = wrapper.build();
        return new FuzzySearchResults(searchResultsWrapper);
    }


}
