package com.stefansavev.similaritysearch;

import com.stefansavev.similaritysearch.implementation.FuzzySearchResultsBuilderWrapper;
import com.stefansavev.similaritysearch.implementation.FuzzySearchResultsWrapper;

import java.util.List;

public class SimilaritySearchResultBuilder {
    boolean inInsertMode = true;
    FuzzySearchResultsBuilderWrapper wrapper = new FuzzySearchResultsBuilderWrapper();

    public void addResult(String queryId, List<SimilaritySearchResult> resultList){
        if (!inInsertMode){
            throw new IllegalStateException("Method build() has already been called.");
        }
        wrapper.addResult(queryId, resultList);
    }

    public SimilaritySearchResults build(){
        if (!inInsertMode){
            throw new IllegalStateException("Method build() has already been called.");
        }
        inInsertMode = false;
        FuzzySearchResultsWrapper searchResultsWrapper = wrapper.build();
        return new SimilaritySearchResults(searchResultsWrapper);
    }


}
