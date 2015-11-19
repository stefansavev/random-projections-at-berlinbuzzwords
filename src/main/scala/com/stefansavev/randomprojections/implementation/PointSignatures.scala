package com.stefansavev.randomprojections.implementation

import com.stefansavev.randomprojections.datarepr.sparse.SparseVector

class SignatureVectors(val signatureVectors: Array[SparseVector]){
  def numSignatures: Int = signatureVectors.size

  def computePointSignatures(query: Array[Double]): Array[Long] = {
    val signatureVectors = this.signatureVectors
    val numSignatures = signatureVectors.size
    val signatures = Array.ofDim[Long](numSignatures)
    var i = 0
    while(i < numSignatures){
      signatures(i) = Signatures.computePointSignature(signatureVectors(i), query)
      i += 1
    }
    signatures
  }

  def computePointSignatures(query: Array[Double], fromIndex: Int, toIndex: Int): Array[Long] = {
    val signatureVectors = this.signatureVectors
    val signatures = Array.ofDim[Long](toIndex - fromIndex)
    var i = fromIndex
    var j = 0
    while(i < toIndex){
      signatures(j) = Signatures.computePointSignature(signatureVectors(i), query)
      i += 1
      j += 1
    }
    signatures
  }
}

//TODO: maybe reorganize the arrays to be by pointid =? [sig1, sig2, ...] and not by sig
class PointSignatures(val pointSignatures: Array[Array[Long]]) {
  def overlap(querySig: Array[Long], pointId: Int): Int = {
    val pointSignatures = this.pointSignatures
    val len = querySig.size
    var sum = 0
    var i = 0
    while(i < len){
      sum += Signatures.overlap(querySig(i), pointSignatures(i)(pointId))
      i += 1
    }
    sum
  }

  def pointSignature(pointId: Int): Array[Long] = {
    val output = Array.ofDim[Long](pointSignatures.length)
    var i = 0
    while(i < pointSignatures.length){
      output(i) = pointSignatures(i)(pointId)
      i += 1
    }
    output
  }

  def overlap(querySig: Array[Long], pointId: Int, fromIndex: Int, toIndex: Int): Int = {
    //TODO: it will be good to align the accesses (store all signature longs consequtively)
    val pointSignatures = this.pointSignatures
    val len = toIndex - fromIndex //should be equal to querySig.size
    var sum = 0
    var i = 0
    var j = fromIndex
    while(i < len){
      sum += Signatures.overlap(querySig(i), pointSignatures(j)(pointId))
      i += 1
      j += 1
    }
    sum
  }

}
