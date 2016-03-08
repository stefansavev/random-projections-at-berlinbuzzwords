package com.stefansavev.similaritysearch;

public class SimilaritySearchResult {
    String name;
    int label;
    double cosineSimilarity;

    public SimilaritySearchResult(String name, int label, double cosineSimilarity) {
        this.name = name;
        this.label = label;
        this.cosineSimilarity = cosineSimilarity;
    }

    public int getLabel() {
        return label;
    }

    public String getName() {
        return name;
    }

    public double getCosineSimilarity() {
        return cosineSimilarity;
    }
}
