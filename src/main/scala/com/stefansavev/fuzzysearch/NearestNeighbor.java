package com.stefansavev.fuzzysearch;

public class NearestNeighbor {
    String name;
    int label;
    double cosineSimilarity;

    public NearestNeighbor(String name, int label, double cosineSimilarity){
        this.name = name;
        this.label = label;
        this.cosineSimilarity = cosineSimilarity;
    }

    public int getLabel(){
        return label;
    }

    public String getName(){
        return name;
    }

    public double getCosineSimilarity(){
        return cosineSimilarity;
    }
}
