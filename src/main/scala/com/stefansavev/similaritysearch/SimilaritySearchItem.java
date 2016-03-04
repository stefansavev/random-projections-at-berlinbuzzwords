package com.stefansavev.similaritysearch;

public class SimilaritySearchItem {
    private double[] vec;
    private String name;
    private int label;

    public SimilaritySearchItem(String name, int label, double[] vec){
        this.name = name;
        this.label = label;
        this.vec = vec;
    }

    public String getName(){
        return name;
    }

    public double[] getVector(){
        return vec;
    }

    public int getLabel(){
        return label;
    }
}
