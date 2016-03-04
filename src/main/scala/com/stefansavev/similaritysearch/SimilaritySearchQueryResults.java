package com.stefansavev.similaritysearch;

import java.util.List;

public class SimilaritySearchQueryResults {
    private String queryId;
    List<SimilaritySearchResult> results;

    public String getName(){
        return queryId;
    }

    public List<SimilaritySearchResult> getQueryResults(){
        return results;
    }

    public SimilaritySearchQueryResults(String queryId, List<SimilaritySearchResult> results){
        this.queryId = queryId;
        this.results = results;
    }

    private List<SimilaritySearchResult> getResults(){
        return results;
    }
}
