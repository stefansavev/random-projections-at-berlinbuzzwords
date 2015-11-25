package com.stefansavev.randomprojections.dimensionalityreduction.svd

import java.io.PrintWriter
import java.util.Random

import com.github.fommil.netlib.BLAS
import com.stefansavev.randomprojections.datarepr.dense.{DenseRowStoredMatrixViewBuilderFactory, ColumnHeaderBuilder, DataFrameView}
import com.stefansavev.randomprojections.dimensionalityreduction.interface.{DimensionalityReductionTransform, DimensionalityReductionParams}
import com.stefansavev.randomprojections.utils.{Utils, DenseMatrixUtils}
import no.uib.cipr.matrix.{SVD => MatrixSVD, DenseMatrix}

trait SVDMethod{
  def fit(params: SVDParams, dataFrame: DataFrameView): SVDTransform
}

object SVDUtils{

  def datasetToDenseMatrix(dataFrame: DataFrameView): DenseMatrix = {
    val numCols = dataFrame.numCols
    val numRows = dataFrame.numRows
    val denseMatrix = new DenseMatrix(numRows, numCols)
    var pointId = 0
    while(pointId < dataFrame.numRows){
      val dataPoint = dataFrame.getPointAsDenseVector(pointId)
      var i = 0
      while(i < numCols){
        denseMatrix.set(pointId, i, dataPoint(i))
        i += 1
      }
      pointId += 1
    }
    denseMatrix
  }

  def divideEntriesBy(summary: DenseMatrix, denom: Double): Unit = {
    val numRows = summary.numRows()
    val numCols = summary.numColumns()
    var i = 0
    while(i < summary.numRows()){
      var j = 0
      while(j < numCols){
        val value = summary.get(i, j)
        summary.set(i, j, value/denom)
        j += 1
      }
      i += 1
    }
  }

  def weightVt(Vt: DenseMatrix, s: Array[Double]): Unit = {

    def computeWeight(s_i: Double): Double = {
      1.0 //there are some reasonable weightings but they don't matter so much
    }

    var i = 0
    while(i < Vt.numRows()){
      //multiply each row
      val w = computeWeight(s(i))
      var j = 0
      while(j < Vt.numColumns()){
        val value = Vt.get(i, j)
        Vt.set(i, j, w*value)
        j += 1
      }
      i += 1
    }
  }

  def dumpVt(fileName: String, Vt: DenseMatrix): Unit = {
    val printWriter = new PrintWriter(fileName)
    var i = 0
    while(i < Vt.numRows()){
      var j = 0
      while(j < Vt.numColumns()){
        val value = Vt.get(i, j)
        printWriter.write(value.toString)
        if (j + 1 < Vt.numColumns()){
          printWriter.write(" ")
        }
        j += 1
      }
      if (i + 1 < Vt.numRows()){
        printWriter.write("\n")
      }
      i += 1
    }
  }
}

object FullDenseSVD extends SVDMethod {
  def XtTimesX(X: DenseMatrix): DenseMatrix = {
    val numCols = X.numColumns()
    val numRows = X.numRows()
    val output = new DenseMatrix(numCols, numCols)
    val data = X.getData
    //for the interface see http://www.math.utah.edu/software/lapack/lapack-blas/dgemm.html
    BLAS.getInstance.dgemm(
      "T", /*use X', not X*/
      "N", /*use X, not X'*/
      numCols, /*m: first dimension (num rows) of X'*/
      numCols, /*m: number of columns of X*/
      numRows, /*number of columns of X', number of rows of X*/
      1.0,
      data /*A = X*/,
      numRows /*first dimension of X*/,
      data /*B = X*/,
      numRows /*first dimension of X*/,
      1,
      output.getData,
      numCols /*first dimension of output*/)
    //returns C
    output
  }


  def fit(params: SVDParams, dataFrame: DataFrameView): SVDTransform = {
    println("Using SVD on the full data")
    val X = SVDUtils.datasetToDenseMatrix(dataFrame)
    val numCols = dataFrame.numCols
    val output = XtTimesX(X)
    val denom = dataFrame.numRows.toDouble - 1.0
    SVDUtils.divideEntriesBy(output.asInstanceOf[DenseMatrix], denom)
    val svd = MatrixSVD.factorize(output)
    val s = svd.getS()
    var i = 0
    val Vt = svd.getVt
    //SVDUtils.weightVt(Vt, svd.getS) //now Vt is weighted //optional
    new SVDTransform(params.k, Vt)
  }
}

object SVDFromRandomizedDataEmbedding extends SVDMethod{
  def computeSummary(numProj: Int, rnd: Random, dataFrame: DataFrameView): DenseMatrix = {
    val numCols = dataFrame.numCols
    val summary = new DenseMatrix(numProj, numCols)
    var pointId = 0
    while(pointId < dataFrame.numRows){
      val dataPoint = dataFrame.getPointAsDenseVector(pointId)
      val sign = rnd.nextGaussian()
      val index = rnd.nextInt(numProj)
      var i = 0
      while(i < numCols){
        //val value = sign * dataPoint(i) + summary.get(index, i)
        //summary.set(index, i, value)
        //optionally can add to a couple of index positions
        summary.add(index, i, sign * dataPoint(i))
        i += 1
      }
      pointId += 1
    }
    val denom = Math.sqrt(dataFrame.numRows - 1)//why this one?
    SVDUtils.divideEntriesBy(summary, denom)
    summary
  }

  def fit(params: SVDParams, dataFrame: DataFrameView): SVDTransform = {
    //todo: while doing this pass does it make sense to also do something with the columns
    //also if the feature space is too large (do the hashing trick???)
    val rnd = new Random(28841)
    val numProj = Math.min(1000, dataFrame.numRows)
    val summary = computeSummary(numProj, rnd, dataFrame)
    println("Computed dataset summary using one pass")
    //val summary = computeSummary2(rnd, dataFrame, 200, 20) //it should be that 2*200 < numCols
    //may be divide by num rows
    val numCols = dataFrame.numCols
    val result = new DenseMatrix(numCols, numCols)
    //summary.transABmult(summary, result)

    val svd = MatrixSVD.factorize(summary)
    val s = svd.getS()
    var i = 0
    val Vt = svd.getVt
    //SVDUtils.weightVt(Vt, svd.getS) //now Vt is weighted //optional
    new SVDTransform(params.k, Vt)
    //dumpVt("D:/tmp/debug-vt.txt", svd.getVt)
    //do the SVD of the summary matrix

    /*
    val svd = SVD.factorize(mat)
    val s = svd.getS
    i = 0
    for(v <- s){
      println(i + ":" + v)
      i += 1
    }
    */
    //projectDataset(k, svd.getVt, dataFrame) //second pass
  }
}
//-1 means choose k optimally
case class SVDParams(k: Int, svdMethod: SVDMethod) extends DimensionalityReductionParams{
}

class SVDTransform(val k: Int, val weightedVt: DenseMatrix) extends DimensionalityReductionTransform{
  def reduceToTopK(newK: Int): SVDTransform = {
    if (newK == k){
      this
    }
    else if (newK < k){
      new SVDTransform(newK, DenseMatrixUtils.takeRows(weightedVt, newK))
    }
    else{
      Utils.internalError()
    }
  }

  private def projectOnToRowsVt(k: Int, vec: Array[Double], weightedVt: DenseMatrix, out: Array[Double]): Unit = {
    val numOrigFeatures = weightedVt.numColumns()
    var i = 0
    while(i < k){//for each new feature
    //for each feature in the original data point
    var dot = 0.0
      var j = 0
      while(j < numOrigFeatures){
        dot += vec(j)*weightedVt.get(i, j)
        j += 1
      }
      out(i) = dot
      i += 1
    }
  }

  def transformQuery(query: Array[Double]): Array[Double] = {
    val out = Array.ofDim[Double](this.k)
    projectOnToRowsVt(this.k, query, this.weightedVt, out)
    out
  }

  def projectDataset(dataFrame: DataFrameView): DataFrameView = {
    val k = this.k
    val oldHeader = dataFrame.rowStoredView.getColumnHeader
    val newNumCols = k
    val newF = Array.range(0, newNumCols).map(i => (i.toString,i ))
    val header = ColumnHeaderBuilder.build(oldHeader.labelName, newF, false, dataFrame.rowStoredView.getColumnHeader.getStoreBuilderType())
    val builder = DenseRowStoredMatrixViewBuilderFactory.create(header)

    val colIds = Array.range(0, dataFrame.numCols)
    val vec = Array.ofDim[Double](dataFrame.numCols)
    val output = Array.ofDim[Double](k)

    val newIds = Array.range(0, newNumCols)
    var i = 0
    while(i < dataFrame.numRows){
      dataFrame.getPointAsDenseVector(i, colIds, vec)
      projectOnToRowsVt(k, vec, weightedVt, output)
      if (i % 5000 == 0){
        println("Processed " + i + " rows")
      }
      builder.addRow(dataFrame.getLabel(i), newIds, output)
      i += 1
    }

    val indexes = dataFrame.indexes
    new DataFrameView(indexes, builder.build())
  }
}

object SVD {

  def fit(params: SVDParams, dataFrame: DataFrameView): SVDTransform = {
    params.svdMethod.fit(params, dataFrame)
  }

  def transform(svdTransform: SVDTransform, inputData: DataFrameView): DataFrameView = {
    println("Started transformation with SVD")
    val result = svdTransform.projectDataset(inputData)
    println("Finished transformation with SVD")
    result
  }
}

trait OnlineFitter{
  def pass(vec: Array[Double]): Unit
  def finalizeFit(): DimensionalityReductionTransform
}

trait OnlineTransform{
  def getFitter(): OnlineFitter
}

class OnlineSVDFitter(numOrigColumns: Int) extends OnlineFitter{
  val maxRows = 100000
  var numRowsInStore = 0
  var totalRows = 0.0
  var currentStore = new DenseMatrix(maxRows, numOrigColumns)
  val XtX = new DenseMatrix(numOrigColumns, numOrigColumns)

  def pass(vec: Array[Double]): Unit = {
    DenseMatrixUtils.addToRow(currentStore, numRowsInStore, vec)
    numRowsInStore += 1

    if (numRowsInStore == maxRows){
      val partialXtX = FullDenseSVD.XtTimesX(currentStore)
      DenseMatrixUtils.addMatrixTo(XtX, partialXtX)
      totalRows += numRowsInStore
      //reset rows
      numRowsInStore = 0
    }
  }

  def finalizeFit(): DimensionalityReductionTransform = {
    if (numRowsInStore > 0){
      val partialStore = DenseMatrixUtils.takeRows(currentStore, numRowsInStore)
      val partialXtX = FullDenseSVD.XtTimesX(partialStore)
      DenseMatrixUtils.addMatrixTo(XtX, partialXtX)
      totalRows += numRowsInStore
    }
    numRowsInStore = -1
    currentStore = null //invalidate the object
    val denom = totalRows - 1.0
    SVDUtils.divideEntriesBy(XtX, denom)
    val svd = MatrixSVD.factorize(XtX)
    val s = svd.getS()
    val Vt = svd.getVt
    new SVDTransform(numOrigColumns, Vt)
  }
}

class OnlineSVD(k: Int) extends OnlineTransform{
  def getFitter(): OnlineFitter = null
}

object FullDenseSVDLimitedMemory extends SVDMethod{
  def fit(params: SVDParams, dataFrame: DataFrameView): SVDTransform = {
    val fitter = new OnlineSVDFitter(dataFrame.numCols)
    var i = 0
    while(i < dataFrame.numRows){
      fitter.pass(dataFrame.getPointAsDenseVector(i))
      i += 1
    }

    val svdTransform = fitter.finalizeFit().asInstanceOf[SVDTransform]
    val k = params.k
    new SVDTransform(k, DenseMatrixUtils.takeRows(svdTransform.weightedVt, k))
  }
}