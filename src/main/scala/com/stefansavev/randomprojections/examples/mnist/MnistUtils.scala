package com.stefansavev.randomprojections.examples.mnist

import com.stefansavev.randomprojections.implementation.KNNS
import java.io.PrintWriter
import com.stefansavev.randomprojections.datarepr.dense.{RowStoredMatrixView, DenseRowStoredMatrixViewBuilderFactory, DataFrameOptions, DataFrameView}
import com.stefansavev.randomprojections.file.CSVFileOptions

object MnistUtils {

  def loadData(fileName: String): DataFrameView = {
    val opt = CSVFileOptions(onlyTopRecords = None)
    val dataFrameOptions = new DataFrameOptions(labelColumnName = "label", builderFactory = DenseRowStoredMatrixViewBuilderFactory, normalizeVectors = true)
    RowStoredMatrixView.fromFile(fileName, opt, dataFrameOptions)
  }

  def writePredictionsInKaggleFormat(outputFile: String, allKnns: Array[KNNS]): Unit = {
    val writer = new PrintWriter(outputFile)
    writer.println("ImageId,Label")
    var i = 0
    while(i < allKnns.length){
      val topNNLabel = allKnns(i).neighbors(0).label
      val pointId = i + 1
      val asStr = s"${pointId},${topNNLabel}"
      writer.println(asStr)
      i += 1
    }
    writer.close()
  }
}
