package com.stefansavev.fuzzysearch;

public class FuzzySearchItem {
    private double[] vec;
    private String name;
    private int label;

    public FuzzySearchItem(String name, int label, double[] vec){
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
