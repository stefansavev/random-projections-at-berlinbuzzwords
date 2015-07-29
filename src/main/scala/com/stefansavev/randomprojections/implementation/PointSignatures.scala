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
}
