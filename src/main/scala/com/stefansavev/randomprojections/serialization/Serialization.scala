package com.stefansavev.randomprojections.serialization

import java.io._

import com.stefansavev.core.serialization._
import com.stefansavev.core.serialization._
import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.utils.Utils
import com.typesafe.scalalogging.StrictLogging

object BinaryFileSerializerSig {
  val signature = Array(80, 42, 51, 67)

  def isValidSignature(arr: Array[Int]): Boolean = {
    if (arr.length != signature.length)
      false
    else {
      !arr.zip(signature).exists({ case (found, expected) => found != expected })
    }
  }
}

class BinaryFileSerializer(file: File) {

  import java.nio.ByteBuffer

  val stream = new BufferedOutputStream(new FileOutputStream(file))
  val b = ByteBuffer.allocate(4)
  putIntArray(BinaryFileSerializerSig.signature)

  def putInt(i: Int): Unit = {
    IntSerializer.write(stream, i)
  }

  def putIntArrays(arrs: Array[Int]*): Unit = {
    for (arr <- arrs) {
      putIntArray(arr)
    }
  }

  def putIntArray(arr: Array[Int]): Unit = {
    //TODO: make more efficient
    putInt(arr.length)
    var i = 0
    while (i < arr.length) {
      putInt(arr(i))
      i += 1
    }
  }

  def close(): Unit = {
    stream.close()
  }
}

object DataFrameViewSerializer {
  val serializer = DataFrameViewSerializers.dataFrameSerializer()

  def toBinary(outputStream: OutputStream, dataFrameView: DataFrameView): Unit = {
    serializer.toBinary(outputStream, dataFrameView)
  }

  def fromBinary(inputStream: InputStream): DataFrameView = {
    serializer.fromBinary(inputStream)
  }
}

object PointSignaturesSerializer extends StrictLogging {

  def toBinary(outputStream: OutputStream, pointSignatures: PointSignatures): Unit = {
    //TODO: this functionality should be split into 2 classes
    if (pointSignatures.backingDir != null && pointSignatures.pointSigReference == null) {
      IntSerializer.write(outputStream, 1) //case 1
      StringSerializer.write(outputStream, pointSignatures.backingDir)
      IntSerializer.write(outputStream, pointSignatures.numPartitions)
      IntSerializer.write(outputStream, pointSignatures.numPoints)
      IntSerializer.write(outputStream, pointSignatures.numSignatures)
    }
    else if (pointSignatures.pointSigReference != null) {
      IntSerializer.write(outputStream, 3) //case 3
      val pointSignatureSer = RandomTreesSerializersV2.pointSignatureReferenceSerializer()
      pointSignatureSer.toBinary(outputStream, pointSignatures.pointSigReference)
      IntSerializer.write(outputStream, pointSignatures.numPoints)
      IntSerializer.write(outputStream, pointSignatures.numSignatures)
    }
    else {
      IntSerializer.write(outputStream, 2) //case 2
      LongArraySerializer.write(outputStream, pointSignatures.pointSignatures)
      IntSerializer.write(outputStream, pointSignatures.numPoints)
      IntSerializer.write(outputStream, pointSignatures.numSignatures)
    }

    /*
    val signatures = pointSignatures.pointSignatures
    val len = signatures.length
    outputStream.writeInt(len)
    var i = 0
    while(i < len){
      LongArraySerializer.write(outputStream, signatures(i))
      i += 1
    }
    */
  }

  def fromBinary(inputStream: InputStream): PointSignatures = {
    val caseId = IntSerializer.read(inputStream)
    if (caseId == 1) {
      val backingDir = StringSerializer.read(inputStream)
      val numPartitions = IntSerializer.read(inputStream)

      val numPoints = IntSerializer.read(inputStream)
      val numSig = IntSerializer.read(inputStream)
      val data = Array.ofDim[Long](numPoints*numSig)
      var offset = 0

      Utils.timed("Read Signature Vectors", {
        var i = 0
        while (i < numPartitions) {
          val subStream = new BufferedInputStream(new FileInputStream(DiskBackedOnlineSignatureVectorsUtils.fileName(backingDir, i)))
          val output = LongArraySerializer.read(subStream)
          subStream.close()
          System.arraycopy(output, 0, data, offset, output.length)
          offset += output.length
          i += 1
        }
      })(logger)

      if (offset != data.length) {
        Utils.internalError()
      }
      new PointSignatures(null, null, -1, data, numPoints, numSig)
    }
    else if (caseId == 2) {
      val signatures = LongArraySerializer.read(inputStream)
      val numPoints = IntSerializer.read(inputStream)
      val numSignatures = IntSerializer.read(inputStream)
      new PointSignatures(null, null, -1, signatures, numPoints, numSignatures)
    }
    else if (caseId == 3) {
      val pointSignatureSer = RandomTreesSerializersV2.pointSignatureReferenceSerializer()
      val pointSigRef = pointSignatureSer.fromBinary(inputStream)
      val pointSig = pointSigRef.toPointSignatures()
      val numPoints = IntSerializer.read(inputStream)
      val numSig = IntSerializer.read(inputStream)
      if (pointSig.numSignatures != numSig) {
        Utils.internalError()
      }
      if (pointSig.numPoints != numPoints) {
        Utils.internalError()
      }
      pointSig
    }
    else {
      Utils.internalError()
    }
    /*
    val len = inputStream.readInt()
    val vectors = Array.ofDim[Array[Long]](len)
    var i = 0
    while(i < len){
      vectors(i) = LongArraySerializer.read(inputStream)
      i += 1
    }
    new PointSignatures(vectors)
    */
  }
}

//TODO: use sparse vector serialializer
object ProjectionVectorSerializer {

  import StreamExtensions._

  def toBinary(outputStream: OutputStream, projVec: AbstractProjectionVector): Unit = {
    val vec = projVec.asInstanceOf[HadamardProjectionVector].signs
    outputStream.writeInt(vec.dim)
    val len = vec.ids.length
    outputStream.writeInt(len)
    var i = 0
    while (i < len) {
      outputStream.writeInt(vec.ids(i))
      i += 1
    }
    i = 0
    while (i < len) {
      DoubleSerializer.write(outputStream, vec.values(i))
      i += 1
    }
  }

  def fromBinary(inputStream: InputStream): AbstractProjectionVector = {
    val dim = inputStream.readInt()
    val len = inputStream.readInt()
    val ids = Array.ofDim[Int](len)
    val values = Array.ofDim[Double](len)

    var i = 0
    while (i < len) {
      ids(i) = inputStream.readInt()
      i += 1
    }

    i = 0
    while (i < len) {
      values(i) = DoubleSerializer.read(inputStream)
      i += 1
    }

    new HadamardProjectionVector(new SparseVector(dim, ids, values))
  }
}





