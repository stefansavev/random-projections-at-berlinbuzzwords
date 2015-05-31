package com.stefansavev.randomprojections.datarepr.dense

case class DataFrameOptions(labelColumnName: String,
                            normalizeVectors: Boolean, //whether to divide by squared length
                            builderFactory: RowStoredMatrixViewBuilderFactory,
                            rowTransformerBuilder: RowTransformerBuilder = NoRowTransformerBuilderProvided){
}

