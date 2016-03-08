package com.stefansavev.similaritysearch;

public interface QueryType {
    class CosineSimilarity implements QueryType {

    }

    class QueryLikelihood implements QueryType {
        public QueryLikelihood() {
            throw new UnsupportedOperationException();
        }
    }
}
