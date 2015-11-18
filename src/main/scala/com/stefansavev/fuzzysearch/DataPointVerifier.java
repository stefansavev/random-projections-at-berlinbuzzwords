package com.stefansavev.fuzzysearch;

public class DataPointVerifier {
    public static double[] normalizeDataPointOrFail(double[] dataPoint){
        double sumOfSquares = 0.0;
        for(int i = 0; i < dataPoint.length; i ++){
            double value = dataPoint[i];
            if (Double.isInfinite(value) || Double.isNaN(value)){
                throw new InvalidDataPointException("The data point contains infinite values or NaNs");
            }
            sumOfSquares += value*value;
        }

        if (Double.isInfinite(sumOfSquares) || Double.isNaN(sumOfSquares)){
            throw new InvalidDataPointException("The sum of squares of the data point is not a number of infinite");
        }
        if (sumOfSquares < 1e-6){
            throw new InvalidDataPointException("The sum of squares is too close to zero");
        }
        double[] copy = new double[dataPoint.length];
        double sqrtSumOfSquares = Math.sqrt(sumOfSquares);
        for(int i = 0; i < dataPoint.length; i ++) {
            double value = dataPoint[i];
            copy[i] = value/sqrtSumOfSquares;
        }
        return copy;
    }
}
