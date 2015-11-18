package com.stefansavev.fuzzysearch;

import java.util.List;

public class FuzzySearchQueryResults {
    private String queryId;
    List<FuzzySearchResult> results;

    public String getName(){
        return queryId;
    }

    public List<FuzzySearchResult> getQueryResults(){
        return results;
    }

    public FuzzySearchQueryResults(String queryId, List<FuzzySearchResult> results){
        this.queryId = queryId;
        this.results = results;
    }

    private List<FuzzySearchResult> getResults(){
        return results;
    }
}
