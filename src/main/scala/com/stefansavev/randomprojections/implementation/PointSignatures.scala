package com.stefansavev.randomprojections.implementation

import java.io.{FileInputStream, BufferedInputStream}

import com.stefansavev.randomprojections.actors.Application
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.serialization.core.Core
import com.stefansavev.randomprojections.serialization.core.PrimitiveTypeSerializers.TypedLongArraySerializer
import com.stefansavev.randomprojections.utils.{Utils}

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
/*
class PointSignatures__Old(val pointSignatures: Array[Array[Long]]) {
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
*/

object PointSignatures{
  def fromPreviousVersion(data: Array[Array[Long]]): PointSignatures = {
    val numSignatures = data.length
    val numPoints = data(0).length
    val newData = Array.ofDim[Long](numSignatures*numPoints)
    var pntId = 0
    while(pntId < numPoints){
      var sigId = 0
      while(sigId < numSignatures){
        val sig = data(sigId)(pntId)
        val offset = pntId*numSignatures + sigId
        newData(offset) = sig
        sigId += 1
      }
      pntId += 1
    }
    new PointSignatures(null, null, -1, newData, numPoints, numSignatures)
  }
}

object PointSignatureReference{
  type TupleType = (String, Int, Int, Int, Int, Array[Long])

  def fromTuple(t: TupleType): PointSignatureReference = {
    val (backingDir, numPartitions, numPoints, numSignatures, partitionSize, positions) = t
    new PointSignatureReference(backingDir, numPartitions, numPoints, numSignatures, partitionSize, positions)
  }
}

class PointSignatureReference(backingDir: String, numPartitions: Int, numPoints: Int, numSignatures: Int, partitionSize: Int, positions: Array[Long]){
  def toTuple(): PointSignatureReference.TupleType = {
    //TODO: maybe just calling unapply on a case class would do the trick
    (backingDir, numPartitions, numPoints, numSignatures, partitionSize, positions)
  }

  def toPointSignatures(): PointSignatures = {
    val buffer1 = Array.ofDim[Long](numPoints*numSignatures)

    def readPartition(partId: Int): Array[Long] = {
      val fileName = AsyncSignatureVectorsUtils.fileName(backingDir)
      val inputStream = new BufferedInputStream(new FileInputStream(fileName))
      val fromByte = positions(partId)
      val toByte = positions(partId + 1)
      inputStream.skip(fromByte)
      val size = (toByte - fromByte).toInt
      val input: Array[Byte] = new Array[Byte](size)
      inputStream.read(input)
      inputStream.close()
      val values = Core.fromBytes(TypedLongArraySerializer, input)
      values
    }

    val readSynchronously = true
    val readAsync = false
    if (readSynchronously){
      var offset = 0
      for(i <- 0 until numPartitions){
        val partitionValues = readPartition(i)
        System.arraycopy(partitionValues, 0, buffer1, offset, partitionValues.length)
        println("required offset for part " + i + " " + offset + " " + partitionValues.length)
        offset += partitionValues.length

      }
    }

    //code still in testing
    if (readAsync) {
      val buffer2 = Array.ofDim[Long](numPoints*numSignatures)
      val fileName = AsyncSignatureVectorsUtils.fileName(backingDir)

      //it looks like AsyncFileReader cannot be called multiple times until complete is finished
      val supervisor = Application.createReaderSupervisor("SignatureReaderSupervisor", 10, fileName)
      //val shuffledPartitions = RandomUtils.shuffleInts(new Random(48181), Array.range(0, numPartitions))
      //val handledPartitions = new ArrayBuffer[Int]()
      for (i <- 0 until numPartitions) {
        //val i = j //shuffledPartitions(j)
        println("reading partition " + i)
        //synchronous code
        //val partitionValues = readPartition(i)
        //System.arraycopy(partitionValues, 0, buffer, offset, partitionValues.length)
        val fromByte = positions(i)
        val toByte = positions(i + 1)
        //the read may block, but usually will not block
        supervisor.read(fromByte, toByte, { bytesReceived => {
            if (((toByte - fromByte).toInt) != bytesReceived.length) {
              Utils.internalError()
            }
            //Thread.sleep(5000)
            val partitionId = i //we remember i in the closure
            val values = Core.fromBytes(TypedLongArraySerializer, bytesReceived)
            val offset = partitionId * partitionSize * numSignatures
            //println("computed offset for part " + partitionId + " " + offset + " len: " + values.length)
            System.arraycopy(values, 0, buffer2, offset, values.length)
            for(k <- offset until (offset + values.length)){
              if (buffer1(k) != buffer2(k)){
                println("Error at offset: " + (k - offset))
                Utils.internalError()
              }
            }
            //println("completed array copy at index " + partitionId)
            //handledPartitions += partitionId
          }
        })
        //println("finished read at index " + i)
      }
      supervisor.waitUntilDone()
      //println("# handled partitions " + handledPartitions.toArray.distinct.length + " vs. " + numPartitions)

    }

    new PointSignatures(null, null, -1, buffer1, numPoints, numSignatures)
  }
}

class PointSignatures(val pointSigReference: PointSignatureReference, val backingDir: String, val numPartitions: Int, val pointSignatures: Array[Long], val numPoints: Int, val numSignatures: Int) {
  def overlap(querySig: Array[Long], pointId: Int): Int = {
    val pointSignatures = this.pointSignatures
    val len = querySig.size
    var sum = 0
    var i = 0
    var offset = pointId*numSignatures
    while(i < len){
      sum += Signatures.overlap(querySig(i), pointSignatures(offset))
      i += 1
      offset += 1
    }
    sum
  }

  def overlapTwoPoints(pointId1: Int, pointId2: Int, k: Int): Int = {
    val pointSignatures = this.pointSignatures
    val len = Math.min(k, pointSignatures.length)

    var offset1 = pointId1*numSignatures
    var offset2 = pointId2*numSignatures

    var sum = 0
    var i = 0
    while(i < len){
      sum += Signatures.overlap(pointSignatures(offset1), pointSignatures(offset2))
      offset1 += 1
      offset2 += 1
      i += 1
    }
    sum
  }

  def estimatedCosineTwoPoints(pointId1: Int, pointId2: Int, k: Int): Double = {
    /*
      p_agree = overlap/num_sig
      #Source for the following formula:
      # Similarity Estimation Techniques from Rounding Algorithms Moses S. Charikar
      #p_agree = 1.0 - angle/pi
      #solve for angle
      angle = (1.0 - p_agree)*pi
    */
    val overlap = overlapTwoPoints(pointId1, pointId2, k)
    val numSigBits = 64.0*k.toDouble //number of bits in signature
    val p = overlap.toDouble / numSigBits
    val angle = (1.0 - p)*Math.PI
    Math.cos(angle)
  }

  def pointSignature(pointId: Int): Array[Long] = {
    val output = Array.ofDim[Long](pointSignatures.length)
    var i = 0
    var offset = pointId*numSignatures
    while(i < pointSignatures.length){
      output(i) = pointSignatures(offset)
      i += 1
      offset += 1
    }
    output
  }

  def overlap(querySig: Array[Long], pointId: Int, fromIndex: Int, toIndex: Int): Int = {
    val pointSignatures = this.pointSignatures
    val len = toIndex - fromIndex //should be equal to querySig.size
    var sum = 0
    var i = 0
    var offset = fromIndex + pointId*numSignatures
    while(i < len){
      sum += Signatures.overlap(querySig(i), pointSignatures(offset))
      i += 1
      offset += 1
    }
    sum
  }

}