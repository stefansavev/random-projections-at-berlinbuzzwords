package com.stefansavev.randomprojections.serialization

import java.io._
import java.nio.ByteBuffer
import com.stefansavev.randomprojections.actors
import com.stefansavev.randomprojections.actors.AsyncReaderUtils
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.dimensionalityreduction.interface.{NoDimensionalityReductionTransform, DimensionalityReductionTransform}
import com.stefansavev.randomprojections.dimensionalityreduction.svd.SVDTransform
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.datarepr.dense.{DataFrameView, ColumnHeaderImpl, ColumnHeader}
import com.stefansavev.randomprojections.serialization.RandomTreesSerializersV2.{SplitStrategySerializer, SignatureVectorsSerializer, ReportingDistanceEvaluatorSerializer, DimensionalityReductionTransformSerializer}
import com.stefansavev.randomprojections.serialization.core.TypedSerializer
import com.stefansavev.randomprojections.utils.Utils
import no.uib.cipr.matrix.DenseMatrix

object BinaryFileSerializerSig{
  val signature = Array(80,42,51,67)

  def isValidSignature(arr: Array[Int]):Boolean = {
    if (arr.length != signature.length)
      false
    else{
      !arr.zip(signature).exists({ case (found, expected) => found != expected})
    }
  }
}

class BinaryFileSerializer(file: File){

  import java.nio.ByteBuffer
  val stream = new BufferedOutputStream(new FileOutputStream(file))
  val b = ByteBuffer.allocate(4)
  putIntArray(BinaryFileSerializerSig.signature)

  def putInt(i: Int): Unit = {
    IntSerializer.write(stream, i)
  }

  def putIntArrays(arrs: Array[Int]*): Unit = {
    for(arr <- arrs){
      putIntArray(arr)
    }
  }

  def putIntArray(arr: Array[Int]): Unit = {
    //TODO: make more efficient
    putInt(arr.length)
    var i = 0
    while(i < arr.length){
      putInt(arr(i))
      i += 1
    }
  }

  def close(): Unit = {
    stream.close()
  }
}

object IntSerializer{
  val bytes = Array.ofDim[Byte](4)
  def toByteArray(value: Int): Array[Byte] = {
    ByteBuffer.wrap(bytes).putInt(value)
    bytes
  }

  def toInt(bytes:  Array[Byte]): Int = {
    return ByteBuffer.wrap(bytes).getInt()
  }

  def write(outputStream: OutputStream, value: Int): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Int = {
    inputStream.read(bytes)
    toInt(bytes)
  }

  def sizeInBytes: Long = {
    2
  }
}

object ShortSerializer{
  val bytes = Array.ofDim[Byte](2)

  def toByteArray(value: Short): Array[Byte] = {
    ByteBuffer.wrap(bytes).putShort(value)
    bytes
  }

  def toShort(bytes:  Array[Byte]): Short = {
    return ByteBuffer.wrap(bytes).getShort()
  }

  def write(outputStream: OutputStream, value: Short): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Short = {
    inputStream.read(bytes)
    toShort(bytes)
  }

  def sizeInBytes: Long = {
    2
  }
}

object FloatSerializer{
  val bytes = Array.ofDim[Byte](4)

  def toByteArray(value: Float): Array[Byte] = {
    ByteBuffer.wrap(bytes).putFloat(value)
    bytes
  }

  def toShort(bytes:  Array[Byte]): Float = {
    return ByteBuffer.wrap(bytes).getFloat()
  }

  def write(outputStream: OutputStream, value: Float): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Float = {
    inputStream.read(bytes)
    toShort(bytes)
  }

  def sizeInBytes: Long = {
    4
  }
}

object StringSerializer{

  def write(outputStream: OutputStream, value: String): Unit = {
    IntSerializer.write(outputStream, value.length)
    val buffer = ByteBuffer.allocate(value.length*2)
    var i = 0
    while(i < value.length){
      buffer.putChar(value(i))
      i += 1
    }
    buffer.rewind()
    outputStream.write(buffer.array())
  }

  def read(inputStream: InputStream): String = {
    val len = IntSerializer.read(inputStream)
    val buffer = Array.ofDim[Byte](len*2)
    inputStream.read(buffer)
    val byteBuffer = ByteBuffer.wrap(buffer)
    val output = Array.ofDim[Char](len)
    var i = 0
    while(i < len){
      output(i) = byteBuffer.getChar()
      i += 1
    }
    new String(output)
  }

  def sizeInBytes(input: String): Long = {
    4 + 2*input.length
  }
}

object DoubleSerializer{
  val bytes = Array.ofDim[Byte](8)

  def toByteArray(value: Double): Array[Byte] = {
    ByteBuffer.wrap(bytes).putDouble(value)
    bytes
  }

  def toDouble(bytes:  Array[Byte]): Double = {
    return ByteBuffer.wrap(bytes).getDouble()
  }

  def write(outputStream: OutputStream, value: Double): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Double = {
    inputStream.read(bytes)
    toDouble(bytes)
  }

  def sizeInBytes: Long = {
    8
  }
}

object LongSerializer{
  val bytes = Array.ofDim[Byte](8)

  def toByteArray(value: Long): Array[Byte] = {
    ByteBuffer.wrap(bytes).putLong(value)
    bytes
  }

  def toLong(bytes:  Array[Byte]): Long = {
    return ByteBuffer.wrap(bytes).getLong()
  }

  def write(outputStream: OutputStream, value: Long): Unit = {
    outputStream.write(toByteArray(value))
  }

  def read(inputStream: InputStream): Long = {
    inputStream.read(bytes)
    toLong(bytes)
  }

  def sizeInBytes: Long = {
    4
  }
}

object DoubleArraySerializer{

  def writeToBufferWithoutArrayLength(values: Array[Double], output: Array[Byte]): Unit = {
    val buff = ByteBuffer.wrap(output)
    var i = 0
    while(i < values.length){
      buff.putDouble(values(i))
      i += 1
    }
  }

  def write(outputStream: OutputStream, values: Array[Double]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      DoubleSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Double] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Double](len)
    var i = 0
    while(i < len){
      values(i) = DoubleSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Double]): Long = {
    IntSerializer.sizeInBytes + DoubleSerializer.sizeInBytes*input.length
  }
}

object IntArraySerializer{
  def write(outputStream: OutputStream, values: Array[Int]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      IntSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Int] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Int](len)
    var i = 0
    while(i < len){
      values(i) = IntSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Int]): Long = {
    IntSerializer.sizeInBytes + IntSerializer.sizeInBytes*input.length
  }
}


object FloatArraySerializer{
  def write(outputStream: OutputStream, values: Array[Float]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      FloatSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Float] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Float](len)
    var i = 0
    while(i < len){
      values(i) = FloatSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Float]): Long = {
    IntSerializer.sizeInBytes + FloatSerializer.sizeInBytes*input.length
  }
}

object ByteArraySerializer{
  def write(outputStream: OutputStream, values: Array[Byte]): Unit = {
    IntSerializer.write(outputStream, values.length)
    outputStream.write(values)
  }

  def read(inputStream: InputStream): Array[Byte] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Byte](len)
    inputStream.read(values)
    values
  }

  def sizeInBytes(input: Array[Byte]): Long = {
    IntSerializer.sizeInBytes + input.length
  }
}

object ShortArraySerializer{
  def write(outputStream: OutputStream, values: Array[Short]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      ShortSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Short] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Short](len)
    var i = 0
    while(i < len){
      values(i) = ShortSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Short]): Long = {
    IntSerializer.sizeInBytes + ShortSerializer.sizeInBytes*input.length
  }
}

class AsyncLongArraySerializer(output: Array[Long], offset: Int){
  //output = request how much more
  var state = 0
  var remainingElements = 0
  var outputPos: Int = offset

  def readNextItems(buffer: ByteBuffer, n: Int): Unit = {
    var i = 0
    var j = outputPos
    val output = this.output
    while(i < n){
      output(j) = buffer.getLong
      i += 1
      j += 1
    }
    outputPos = j
    remainingElements -= n
  }

  def process(bytes: Array[Byte], fromPos: Int, toPos: Int): Int = {
    val buffer = ByteBuffer.wrap(bytes)
    buffer.position(fromPos)
    if (state == 0){
      remainingElements = buffer.getInt
      if (remainingElements < 0){
        Utils.internalError()
      }
      state = 1
      val available = Math.min((toPos - fromPos - 4)/8 , remainingElements)
      readNextItems(buffer, available)
      8*(remainingElements - available)
    }
    else{
      val available = Math.min((toPos - fromPos)/8, remainingElements)
      readNextItems(buffer, available)
      8*(remainingElements - available)
    }
  }

  def getArray(): Array[Long] = output

}

object LongArraySerializer{
  def write(outputStream: OutputStream, values: Array[Long]): Unit = {
    IntSerializer.write(outputStream, values.length)
    var i = 0
    while(i < values.length){
      LongSerializer.write(outputStream, values(i))
      i += 1
    }
  }

  def read(inputStream: InputStream): Array[Long] = {
    val len = IntSerializer.read(inputStream)
    val values = Array.ofDim[Long](len)
    var i = 0
    while(i < len){
      values(i) = LongSerializer.read(inputStream)
      i += 1
    }
    values
  }

  def sizeInBytes(input: Array[Long]): Long = {
    IntSerializer.sizeInBytes + LongSerializer.sizeInBytes*input.length
  }
}

object ImplicitSerializers{
  implicit class IntSerializerExt(outputStream: OutputStream) {
    def writeInt(value: Int): Unit = {
      IntSerializer.write(outputStream, value)
    }
  }

  implicit class IntDeSerializerExt(inputStream: InputStream) {
    def readInt(): Int = {
      IntSerializer.read(inputStream)
    }
  }

  implicit class DoubleArraySerializerExt(outputStream: OutputStream) {
    def writeDoubleArray(values: Array[Double]): Unit = {
      DoubleArraySerializer.write(outputStream, values)
    }
  }

  implicit class DoubleArrayDeSerializerExt(inputStream: InputStream) {
    def readDoubleArray(): Array[Double] = {
      DoubleArraySerializer.read(inputStream)
    }
  }

}

object DataFrameViewSerializer{
  val serializer = DataFrameViewSerializers.dataFrameSerializer()
  def toBinary(outputStream: OutputStream, dataFrameView: DataFrameView): Unit = {
    serializer.toBinary(outputStream, dataFrameView)
  }

  def fromBinary(inputStream: InputStream): DataFrameView = {
    serializer.fromBinary(inputStream)
  }
}

object PointSignaturesSerializer{
  import ImplicitSerializers._

  def toBinary(outputStream: OutputStream, pointSignatures: PointSignatures): Unit = {
    //TODO: this functionality should be split into 2 classes
    if (pointSignatures.backingDir != null && pointSignatures.pointSigReference == null){
      IntSerializer.write(outputStream, 1) //case 1
      StringSerializer.write(outputStream, pointSignatures.backingDir)
      IntSerializer.write(outputStream, pointSignatures.numPartitions)
      IntSerializer.write(outputStream, pointSignatures.numPoints)
      IntSerializer.write(outputStream, pointSignatures.numSignatures)
    }
    else if (pointSignatures.pointSigReference != null){
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
    if (caseId == 1){
      val backingDir = StringSerializer.read(inputStream)
      val numPartitions = IntSerializer.read(inputStream)

      val numPoints = IntSerializer.read(inputStream)
      val numSig = IntSerializer.read(inputStream)
      val data = Array.ofDim[Long](numPoints*numSig)
      var offset = 0

      /*
      Utils.timed("asyncRead", {
        val dataV2 = Array.ofDim[Long](numPoints*numSig)
        val fileNames = Array.range(0, numPartitions).map(i => DiskBackedOnlineSignatureVectorsUtils.fileName(backingDir, i))
        var serializers: Array[AsyncLongArraySerializer] = null
        actors.AsyncReaderUtils.readMultipleFiles(fileNames, fileSizes => {
          val fileSizesArray = fileSizes.fileSizes
          val partialSums = Array.ofDim[Long](fileSizesArray.length)

          var i = 1
          while (i < fileSizesArray.length) {
            partialSums(i) = partialSums(0) + (fileSizesArray(0) - 4) / 8
            i += 1
          }
          serializers = Array.range(0, numPartitions).map(i => new AsyncLongArraySerializer(dataV2, partialSums(i).toInt)) //TODO: need to find the offset per file
        },
          results => {
            serializers(results.readerIndex).process(results.bytes, 0, results.bytesRead)
          })
      })
      */
      Utils.timed("SeqRead", {
        var i = 0
        while (i < numPartitions) {
          val subStream = new BufferedInputStream(new FileInputStream(DiskBackedOnlineSignatureVectorsUtils.fileName(backingDir, i)))
          val output = LongArraySerializer.read(subStream)
          println("output.len: " + output.length)
          subStream.close()
          System.arraycopy(output, 0, data, offset, output.length)
          offset += output.length
          i += 1
        }
      })

      if (offset != data.length){
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
    else if (caseId == 3){
      val pointSignatureSer = RandomTreesSerializersV2.pointSignatureReferenceSerializer()
      val pointSigRef = pointSignatureSer.fromBinary(inputStream)
      val pointSig = pointSigRef.toPointSignatures()
      val numPoints = IntSerializer.read(inputStream)
      val numSig = IntSerializer.read(inputStream)
      if (pointSig.numSignatures != numSig){
        Utils.internalError()
      }
      if (pointSig.numPoints != numPoints){
        Utils.internalError()
      }
      pointSig
    }
    else{
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
object ProjectionVectorSerializer{
  import ImplicitSerializers._
  def toBinary(outputStream: OutputStream, projVec: AbstractProjectionVector): Unit = {
    val vec = projVec.asInstanceOf[HadamardProjectionVector].signs
    outputStream.writeInt(vec.dim)
    val len = vec.ids.length
    outputStream.writeInt(len)
    var i = 0
    while(i < len){
      outputStream.writeInt(vec.ids(i))
      i += 1
    }
    i = 0
    while(i < len){
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
    while(i < len){
      ids(i) = inputStream.readInt()
      i += 1
    }

    i = 0
    while(i < len){
      values(i) = DoubleSerializer.read(inputStream)
      i += 1
    }

    new HadamardProjectionVector(new SparseVector(dim, ids, values))
  }
}





