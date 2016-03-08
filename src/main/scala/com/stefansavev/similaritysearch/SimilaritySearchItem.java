package com.stefansavev.similaritysearch;

public class SimilaritySearchItem {
    private double[] vec;
    private String name;
    private int label;
    private byte[] payload; //TODO

    public SimilaritySearchItem(String name, int label, double[] vec, byte[] payload) {
        this.name = name;
        this.label = label;
        this.vec = vec;
        this.payload = payload;
    }

    public SimilaritySearchItem(String name, int label, double[] vec) {
        this(name, label, vec, null);
    }

    public String getName() {
        return name;
    }

    public double[] getVector() {
        return vec;
    }

    public int getLabel() {
        return label;
    }
}
