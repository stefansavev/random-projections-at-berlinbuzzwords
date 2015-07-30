package com.stefansavev.randomprojections.implementation

import java.util.Random

import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector

object Counts{
  def numberOf1s(input: Int): Int = {
    var b = input
    var sum = 0
    var i = 0
    while(i < 8){
      sum += (if (1 == (b & 1)) 1 else 0)
      i += 1
      b >>= 1
    }
    sum
  }

  val table: Array[Int] = Array.range(0, 256).map(i => numberOf1s(i))

  def numberOf1sLong(input: Long): Int = {
    /*
    var h = input
    //println("input:" + input.toBinaryString)

    var sum = 0
    var i = 0
    while(i < 8){
      val part = (input & 0xFFL).toInt
      sum += table(part)
      h >>= 8
      i += 1
    }
    sum
    */

    val i1 = (input & 0xFFL).toInt
    val i2 = ((input >> 8) & 0xFFL).toInt
    val i3 = ((input >> 2*8) & 0xFFL).toInt
    val i4 = ((input >> 3*8) & 0xFFL).toInt

    val i5 = ((input >> 4*8) & 0xFFL).toInt
    val i6 = ((input >> 5*8) & 0xFFL).toInt
    val i7 = ((input >> 6*8) & 0xFFL).toInt
    val i8 = ((input >> 7*8) & 0xFFL).toInt

    val sum = table(i1) + table(i2) + table(i3) + table(i4) + table(i5) + table(i6) + table(i7) + table(i8)
    sum
  }
}
object Signatures {
  //TODO: move to utils
  def generateRandomVector(rnd: Random, numCols: Int, columnIds: Array[Int]): SparseVector = {
    val signs = columnIds.map(_ => (if (rnd.nextDouble() >= 0.5) 1.0 else -1.0))
    var sum = 0.0
    var i = 0
    while(i < signs.length){
      val v = signs(i)
      sum += v*v
      i += 1
    }
    sum = Math.sqrt(sum)

    i = 0
    while(i < signs.length){
      signs(i) /= sum
      i += 1
    }

    val sparseVec = new SparseVector(numCols, columnIds,signs)
    sparseVec
  }

  def getSign(v: Double): Int = if (v >= 0) 1 else -1
  def isSignificant(v: Double): Boolean = Math.abs(v)> Math.sqrt(1.0/64)
  def debugOverlap(signs: SparseVector, q1: Array[Double], q2: Array[Double],s1: Long, s2: Long): Double = {
    val r1 = hadamardRepr(signs, q1)
    val r2 = hadamardRepr(signs, q2)
    var i = 0
    var dotProd = 0.0
    while(i < Math.min(64, r1.length)){
      val signsAgree1 = if (getSign(r1(i))==getSign(r2(i))) 1.0 else 0.0
      val signsAgree = if (isSignificant(r1(i)) && isSignificant(r2(i)) && getSign(r1(i))==getSign(r2(i))) 1.0 else 0.0
      dotProd += (r1(i)*r2(i)) //signsAgree1 + 2.0*signsAgree //(r1(i)*r2(i))
      i += 1
    }
    dotProd

    val h1 = computePointSignature(signs, q1)
    val h2 = computePointSignature(signs, q2)
    if (s1 != h1){
      println("error 1")
    }
    if (s2 != h2){
      println("error 2")
    }
    val v1 = hashToVector(s1)
    val v2 = hashToVector(s2)
    i = 0
    var dotProd2 = 0.0
    while(i < v1.length){
      val signsAgree = if (getSign(v1(i))==getSign(v2(i))) 1.0 else 0.0
      dotProd2 += signsAgree //(r1(i)*r2(i))
      i += 1
    }
    val o = overlap(s1, s2)
    //println(" " + dotProd + " " + dotProd2 + " " + o)
    //o
    dotProd
  }

  def vectorToHash(vec: Array[Double]): Long = {
    var i = 0
    val len = Math.min(vec.length, 64)
    var hash: Long = 0L
    while(i < len){
      val sign = if (vec(i) >= 0) 1L else 0L
      hash = hash << 1 | sign
      i += 1
    }
    hash
  }

  def hashToVector(h: Long): Array[Double] = {
    val res = Array.ofDim[Double](64)
    var v = h
    var i = 0
    while(i < 64){
      if ((v & 1L) == 1L){
        res(63 - i) = 1.0
      }
      else{
        res(63 - i) = -1.0
      }
      v >>= 1
      i += 1
    }
    res
  }

  def computePointSignature(signs: SparseVector, query: Array[Double]): Long = {
    val dim = signs.dim
    val powOf2 = HadamardUtils.roundUp(dim)
    val input = Array.ofDim[Double](powOf2)
    val output = Array.ofDim[Double](powOf2)
    computePointSignature(signs, query, input, output)
  }

  def hadamardRepr(signs: SparseVector, query: Array[Double]): Array[Double] = {
    val dim = signs.dim
    val input =  Array.ofDim[Double](dim)
    val output = Array.ofDim[Double](dim)
    //TODO: move to function
    var j = 0
    while (j < signs.ids.length) {
      val index = signs.ids(j)
      val b = signs.values(j)
      val a = query(index)
      input(j) = a * b
      j += 1
    }

    HadamardUtils.multiplyInto(dim, input, output)
    output
  }

  def computePointSignature(signs: SparseVector, query: Array[Double], input: Array[Double], output: Array[Double]): Long = {
    val dim = signs.dim

    //TODO: move to function
    var j = 0
    while(j < 0){
      input(j)= 0.0
      j += 1
    }

    j = 0
    while(j < signs.ids.length){
      val index = signs.ids(j)
      val b = signs.values(j)
      val a = query(index)
      input(j) = a*b
      j += 1
    }

    HadamardUtils.multiplyInto(input.length, input, output)
    val hash = vectorToHash(output)
    hash
  }

  def overlap(h1: Long, h2: Long): Int = {
    val intersection: Long = ~(h1 ^ h2)
    val c = Counts.numberOf1sLong(intersection)
    c
  }

  def dotProduct(h1: Long, h2: Long): Double = {
    val c = overlap(h1, h2)
    (c - 32).toDouble
  }

  def computePointSignatures(numSignatures: Int, rnd: Random, dataFrameView: DataFrameView): (SignatureVectors, PointSignatures) = {
    val vectors = Array.ofDim[SparseVector](numSignatures)
    val signatures = Array.ofDim[Array[Long]](numSignatures)
    var i = 0
    while(i < numSignatures){
      val (vec, sig) = computePointSignaturesHelper(rnd, dataFrameView)
      vectors(i) = vec
      signatures(i) = sig
      i += 1
    }
    (new SignatureVectors(vectors), new PointSignatures(signatures))
  }

  def computePointSignaturesHelper(rnd: Random, dataFrameView: DataFrameView): (SparseVector, Array[Long]) = {
    val dim = dataFrameView.numCols
    val signs = generateRandomVector(rnd, dim, Array.range(0, dim))
    val vec = Array.ofDim[Double](dataFrameView.numCols)

    val powOf2 = HadamardUtils.roundUp(dim)
    val input = Array.ofDim[Double](powOf2)
    val output = Array.ofDim[Double](powOf2)
    val signatures = Array.ofDim[Long](dataFrameView.numRows)
    var i = 0
    while(i < dataFrameView.numRows){
      dataFrameView.getPointAsDenseVector(i, signs.ids, vec)
      val hash = computePointSignature(signs, vec, input, output)
      signatures(i) = hash
      i += 1
    }
    (signs, signatures)
  }
}
