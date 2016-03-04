package com.stefansavev.similaritysearch;

public interface VectorType {
    int getDimension();
    StorageSize getStorageSize();

    enum StorageSize {
        Double,
        Float,
        TwoBytes,
        SingleByte
    }

    //This is the best case for the current indexing engine
    //The other use cases will be reduced to this case
    //after special preprocessing (e.g. after a deconvolution operation)
    class UncorrelatedFeatures implements VectorType {
        int dimension;
        StorageSize storageSize;

        public UncorrelatedFeatures(int dimension, StorageSize storageSize) {
            this.dimension = dimension;
            this.storageSize = storageSize;
        }

        @Override
        public int getDimension() {
            return dimension;
        }

        public StorageSize getStorageSize() {
            return storageSize;
        }
    }

    class TimeCorrelatedFeatures implements VectorType {
        public TimeCorrelatedFeatures(int dimension) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getDimension() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageSize getStorageSize() {
            throw new UnsupportedOperationException();
        }
    }

    class SpatiallyCorrelatedFeatures implements VectorType {
        public SpatiallyCorrelatedFeatures(int dimension) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getDimension() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageSize getStorageSize() {
            throw new UnsupportedOperationException();
        }
    }
}

