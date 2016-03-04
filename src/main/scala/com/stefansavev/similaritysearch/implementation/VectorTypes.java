package com.stefansavev.similaritysearch.implementation;

import com.stefansavev.similaritysearch.VectorType;

public class VectorTypes {
    public static VectorType uncorrelatedFeatures(int dimension, VectorType.StorageSize storageSize){
        return new VectorType.UncorrelatedFeatures(dimension, storageSize);
    }
}
