package com.stefansavev.randomprojections.implementation

import java.io.PrintWriter
import java.util.Random

import com.stefansavev.randomprojections.datarepr.dense.{ColumnHeaderBuilder, DataFrameView, DenseRowStoredMatrixViewBuilderFactory}
import no.uib.cipr.matrix.{DenseMatrix, SVD}

//scratch file for SVD -- to be removed soon
object SVDTest {
  //temporary file to play around with the SVD
  def test(): Unit = {
    val m = new DenseMatrix(2, 2)
    m.set(0, 0, 1.0)
    m.set(1, 1, 1.0)
    val svd = SVD.factorize(m)
    for (v <- svd.getS()) {
      println(v)
    }
    println("done")
    //svd.
  }

  def svd(dataFrame: DataFrameView): DataFrameView = {
    val numCols = dataFrame.numCols
    val mat = new DenseMatrix(numCols, numCols)
    var pointId = 0
    while (pointId < dataFrame.numRows) {
      val dataPoint = dataFrame.getPointAsDenseVector(pointId)
      var i = 0
      while (i < numCols) {
        var j = 0
        while (j < numCols) {
          val value = mat.get(i, j)
          mat.set(i, j, value + dataPoint(i) * dataPoint(j))
          j += 1
        }
        i += 1
      }
      pointId += 1
    }
    //normalize the sample covariance matrix
    val denom = dataFrame.numRows.toDouble - 1.0
    var i = 0
    while (i < numCols) {
      var j = 0
      while (j < numCols) {
        val value = mat.get(i, j)
        mat.set(i, j, value / denom)
        j += 1
      }
      i += 1
    }

    val lambda = 1.0
    i = 0
    while (i < numCols) {
      val value = mat.get(i, i)
      mat.set(i, i, value + lambda)
      i += 1
    }
    val svd = SVD.factorize(mat)
    val s = svd.getS
    i = 0
    for (v <- s) {
      println(i + ":" + v)
      i += 1
    }
    val k = Math.min(100, s.length)
    val sInv = Array.ofDim[Double](k)
    i = 0
    while (i < k) {
      sInv(i) = s(i) / (s(i) + lambda)
    }
    //weight the patterns
    null
  }

  def divideEntriesBy(summary: DenseMatrix, denom: Double): Unit = {
    val numRows = summary.numRows()
    val numCols = summary.numColumns()
    var i = 0
    while (i < summary.numRows()) {
      var j = 0
      while (j < numCols) {
        val value = summary.get(i, j)
        summary.set(i, j, value / denom)
        j += 1
      }
      i += 1
    }
  }

  def computeSummary(numProj: Int, rnd: Random, dataFrame: DataFrameView): DenseMatrix = {
    val numCols = dataFrame.numCols
    val summary = new DenseMatrix(numProj, numCols)
    var pointId = 0
    while (pointId < dataFrame.numRows) {
      val dataPoint = dataFrame.getPointAsDenseVector(pointId)
      val sign = rnd.nextGaussian()
      val index = rnd.nextInt(numProj)
      var i = 0
      while (i < numCols) {
        //val value = sign * dataPoint(i) + summary.get(index, i)
        //summary.set(index, i, value)
        //optionally can add to a couple of index positions
        summary.add(index, i, sign * dataPoint(i))
        i += 1
      }
      pointId += 1
    }
    val denom = Math.sqrt(dataFrame.numRows - 1) //why this one?
    divideEntriesBy(summary, denom)
    summary
  }

  def weightAndTruncateRows(numProj: Int, numCols: Int, summary: DenseMatrix, s: Array[Double], Vt: DenseMatrix): Unit = {
    val s_mid = s(numProj / 2)
    val delta = s_mid * s_mid
    var i = 0
    while (i < numProj) {
      val s_i2 = Math.sqrt(Math.max(s(i) * s(i) - delta, 0.0))
      if (i < 10) {
        println("s_i " + i + " " + s(i) + " " + s_i2)
      }

      var j = 0
      while (j < numCols) {
        val value = s_i2 * Vt.get(i, j)
        summary.set(i, j, value)
        j += 1
      }
      i += 1
    }
    //i = numProj //it should be
    while (i < 2 * numProj) {
      var j = 0
      while (j < numCols) {
        summary.set(i, j, 0.0)
        j += 1
      }
      i += 1
    }
  }

  def computeSummary2(rnd: Random, dataFrame: DataFrameView, numProj: Int = 1000, expectedEntriesPerProjBeforeSVD: Int = 20): DenseMatrix = {
    val numCols = dataFrame.numCols
    //val L = 100
    val summary = new DenseMatrix(2 * numProj, numCols)
    val resetThreshold = numProj * expectedEntriesPerProjBeforeSVD

    var pointId = 0
    var numProcessedPoints = 0
    while (pointId < dataFrame.numRows) {
      val dataPoint = dataFrame.getPointAsDenseVector(pointId)
      val sign = rnd.nextGaussian()
      //the second offset numProj + ... can be adjusted if before we did not fill up enough rows
      val index = if (numProcessedPoints < resetThreshold) rnd.nextInt(2 * numProj) else numProj + rnd.nextInt(numProj) //initially use the range(0, 2*numProj), but the use only (numProj,2*numProj)
      var i = 0
      while (i < numCols) {
        //val value = sign * dataPoint(i) + summary.get(index, i)
        //summary.set(index, i, value)
        //optionally can add to a couple of index positions
        summary.add(index, i, sign * dataPoint(i))
        i += 1
      }
      pointId += 1
      numProcessedPoints += 1

      if (numProcessedPoints % resetThreshold == 0) {
        //val denom = Math.sqrt(resetThreshold - 1) //??
        //divideEntriesBy(summary, denom)
        //should we normalize the rows of summary???
        println("svd factorize at " + numProcessedPoints)
        val svd = SVD.factorize(summary)
        val s = svd.getS
        val Vt = svd.getVt
        //weight and truncate the rows
        println("truncate rows")
        val denom = Math.sqrt(numProcessedPoints) //expectedEntriesPerProjBeforeSVD
        //divideEntriesBy(summary, denom)
        weightAndTruncateRows(numProj, numCols, summary, s, Vt)
      }
    }
    val denom = Math.sqrt(dataFrame.numRows - 1)
    divideEntriesBy(summary, denom)
    summary
  }

  def weightVt(Vt: DenseMatrix, s: Array[Double]): Unit = {
    //the rows are the projections for embedding in a low dim. space
    //you can do the row multiplied by s
    //the scale of s matters
    val max_s = 10.0 //s(0) worked before
    def computeWeight(s_i: Double): Double = {
      1.0 //s_i/(s_i + max_s) //s_i/(s_i + 100.0)
      //10000.0/((s_i + 10000.0)) //ignore s(why?)
    }

    var i = 0
    while (i < Vt.numRows()) {
      //multiply each row
      val w = computeWeight(s(i))
      var j = 0
      while (j < Vt.numColumns()) {
        val value = Vt.get(i, j)
        Vt.set(i, j, w * value)
        j += 1
      }
      i += 1
    }
  }

  def dumpVt(fileName: String, Vt: DenseMatrix): Unit = {
    val printWriter = new PrintWriter(fileName)
    var i = 0
    while (i < Vt.numRows()) {
      var j = 0
      while (j < Vt.numColumns()) {
        val value = Vt.get(i, j)
        printWriter.write(value.toString)
        if (j + 1 < Vt.numColumns()) {
          printWriter.write(" ")
        }
        j += 1
      }
      if (i + 1 < Vt.numRows()) {
        printWriter.write("\n")
      }
      i += 1
    }
  }

  def projectOnToRowsVt(k: Int, vec: Array[Double], weightedVt: DenseMatrix, out: Array[Double]): Unit = {
    val numOrigFeatures = weightedVt.numColumns()
    var i = 0
    while (i < k) {
      //for each new feature
      //for each feature in the original data point
      var dot = 0.0
      var j = 0
      while (j < numOrigFeatures) {
        dot += vec(j) * weightedVt.get(i, j)
        j += 1
      }
      out(i) = dot
      i += 1
    }
  }

  def projectDataset(k: Int, weightedVt: DenseMatrix, dataFrame: DataFrameView): DataFrameView = {
    val oldHeader = dataFrame.rowStoredView.getColumnHeader
    val newNumCols = k
    val newF = Array.range(0, newNumCols).map(i => (i.toString, i))
    val header = ColumnHeaderBuilder.build(oldHeader.labelName, newF, false)
    val builder = DenseRowStoredMatrixViewBuilderFactory.create(header)

    val colIds = Array.range(0, dataFrame.numCols)
    val vec = Array.ofDim[Double](dataFrame.numCols)
    val output = Array.ofDim[Double](k)

    val newIds = Array.range(0, newNumCols)
    var i = 0
    while (i < dataFrame.numRows) {
      dataFrame.getPointAsDenseVector(i, colIds, vec)
      projectOnToRowsVt(k, vec, weightedVt, output)
      builder.addRow(dataFrame.getLabel(i), newIds, output)
      i += 1
    }

    val indexes = dataFrame.indexes
    new DataFrameView(indexes, builder.build())
  }

  def svdProj(k: Int, dataFrame: DataFrameView): DataFrameView = {
    //todo: while doing this pass does it make sense to also do something with the columns
    //also if the feature space is too large (do the hashing trick???)
    val rnd = new Random(28841)
    val numProj = Math.min(1000, dataFrame.numRows)
    val summary = computeSummary(numProj, rnd, dataFrame)
    //val summary = computeSummary2(rnd, dataFrame, 200, 20) //it should be that 2*200 < numCols
    //may be divide by num rows
    val numCols = dataFrame.numCols
    val result = new DenseMatrix(numCols, numCols)
    //summary.transABmult(summary, result)

    val svd = SVD.factorize(summary)
    val s = svd.getS()
    var i = 0
    for (v <- s) {
      if (i < 100) {
        println(i + ":" + v * v)
      }
      i += 1
    }
    weightVt(svd.getVt, svd.getS) //now Vt is weighted
    dumpVt("D:/tmp/debug-vt.txt", svd.getVt)
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
    projectDataset(k, svd.getVt, dataFrame) //second pass
  }

  def svdProjV2(dataFrame: DataFrameView): DataFrameView = {
    //todo: while doing this pass does it make sense to also do something with the columns
    //also if the feature space is too large (do the hashing trick???)
    val rnd = new Random(28841)
    val numProj = Math.min(2000, dataFrame.numRows)
    val summary = computeSummary(numProj, rnd, dataFrame)

    //may be divide by num rows
    val numCols = dataFrame.numCols
    val result = new DenseMatrix(numCols, numCols)
    //summary.transABmult(summary, result)

    val svd = SVD.factorize(summary)
    val s = svd.getS()
    var i = 0
    for (v <- s) {
      if (i < 100) {
        println(i + ":" + v * v)
      }
      i += 1
    }
    //weightVt(svd.getVt, svd.getS)
    dumpVt("D:/tmp/debug-vt.txt", svd.getVt)
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
    null
  }

}
