package com.stefansavev.randomprojections.datarepr.dense

import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.file.{CSVFile, CSVFileOptions}

trait RowStoredMatrixView{
  def getAllRowNames(): Array[String]
  def getColumnHeader: ColumnHeader
  def numCols: Int
  def getAllLabels(): Array[Int]
  def getLabel(rowId: Int): Int
  def getRowIdByName(name: String): Int
  def getName(rowId: Int): String
  def getPointAsDenseVector(pntId: Int): Array[Double]
  def getPointAsDenseVector(pntId: Int, signs: Array[Int], vec: Array[Double]): Unit

  def dist(id1: Int, id2: Int): Double

  def multiplyRowComponentWiseBySparseVector(pntId: Int, sv: SparseVector, output: Array[Double]): Unit

  def cosineForNormalizedData(query: Array[Double], id: Int): Double
}

object RowStoredMatrixView{

  def fromFile(fileName: String,  opt: CSVFileOptions, dataFrameOptions: DataFrameOptions): DataFrameView = {
    val matrixBuilderFactory = dataFrameOptions.builderFactory
    val normalize = dataFrameOptions.normalizeVectors

    if (!opt.hasHeader){
      throw new IllegalStateException("has header has to be set")
    }

    val file = CSVFile.read(fileName, opt)
    if (!file.header.isDefined){
      throw new IllegalStateException("Header must be present")
    }

    val columnName2Index = file.header.get.zipWithIndex
    val labelColumnName = dataFrameOptions.labelColumnName
    val totalColumnsWithHeader = columnName2Index.length

    val labelIdx = columnName2Index.find({case (key,v) => key == labelColumnName}) match {
      case None => throw new IllegalStateException("Required column with name " + labelColumnName + " was not found")
      case Some((_,index)) => index
    }

    def getTransformer(columnName: String): RowTransformer = {
      dataFrameOptions.rowTransformerBuilder.getTransformer(columnName) match {
        case None => new DoubleTransformer(columnName)
        case Some(value) => value
      }
    }

    val(featureIndexes: Array[(Int,RowTransformer)], columnWithDataFrameIndexes: Array[(String,Int)]) =
      columnName2Index.filter({case (k,v) => k != labelColumnName}).
        sortBy(_._2).zipWithIndex.
        map({case ((columnName, indexInFile), indexInDataFrame) =>
        ((indexInFile, getTransformer(columnName)), (columnName, indexInDataFrame))
      }).unzip
    //val frameBuilder = ByRowDataFrameBuilder(header)
    //def build(_labelName: String, columnIds: Array[(String, Int)]): ColumnHeader = {
    val header = ColumnHeaderBuilder.build(labelColumnName, columnWithDataFrameIndexes, false)
    val builder = matrixBuilderFactory.create(header)

    var numRows = 0

    val labelTransformer = new IntTransformer(labelColumnName)
    for(valuesByLine <- file.getLines()){
      if (totalColumnsWithHeader != valuesByLine.length){
        throw new IllegalStateException("Row should contain " + totalColumnsWithHeader + " columns")
      }
      val line = numRows + 2

      val label = labelTransformer.transformToInt(line, labelIdx, valuesByLine(labelIdx))
      var j = 0
      val indexes = Array.ofDim[Int](featureIndexes.length)
      val values = Array.ofDim[Double](featureIndexes.length)
      while(j < featureIndexes.length){
        val rowTransformer = featureIndexes(j)
        val index = rowTransformer._1
        val transformer = rowTransformer._2
        val value = valuesByLine(index)
        val dblValue = transformer.transformToDouble(line, index, value)
        indexes(j) = j
        values(j) = dblValue
        j += 1
      }
      //val featureValues = featureIndexes.map({case (index, rowTransformer) => rowTransformer. (line,  valuesByLine(i) ))
      def normalizeValues(values: Array[Double]): Array[Double] = {
        val sum = values.map(v=> v* v).sum
        val sqrtSum = Math.sqrt(sum)
        if (sqrtSum > 0.0001)
          values.map(v => v/sqrtSum)
        else
          values
      }

      val normalizedValues = if (normalize) normalizeValues(values) else values
      builder.addRow(label, indexes, normalizedValues)
      numRows += 1
      if (numRows % 1000 == 0){
        println("Line " + numRows)
      }
    }
    file.close()
    builder.build()

    val indexes = PointIndexes(Array.range(0, numRows))
    new DataFrameView(indexes, builder.build())
  }
}