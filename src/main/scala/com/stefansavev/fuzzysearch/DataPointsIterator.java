package com.stefansavev.fuzzysearch;

import java.util.Iterator;

public class DataPointsIterator implements Iterator<FuzzySearchItem> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public FuzzySearchItem next() {
        return null;
    }

    @Override
    public void remove() {
        throw new IllegalStateException("remove is not supported");
    }
}
