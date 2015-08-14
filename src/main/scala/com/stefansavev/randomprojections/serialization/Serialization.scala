package com.stefansavev.randomprojections.serialization

import java.io._
import java.nio.ByteBuffer
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.dimensionalityreduction.interface.{NoDimensionalityReductionTransform, DimensionalityReductionTransform}
import com.stefansavev.randomprojections.dimensionalityreduction.svd.SVDTransform
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.datarepr.dense.{ColumnHeaderImpl, ColumnHeader}
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
}

object DoubleArraySerializer{
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

object SVDTransformSerializer{
  def toBinary(outputStream: OutputStream, svdTransform: SVDTransform): Unit = {
    IntSerializer.write(outputStream, svdTransform.k)
    val matrix = svdTransform.weightedVt
    IntSerializer.write(outputStream, matrix.numRows())
    IntSerializer.write(outputStream, matrix.numColumns())
    DoubleArraySerializer.write(outputStream, matrix.getData)
  }

  def fromBinary(inputStream: InputStream): SVDTransform = {
    val k = IntSerializer.read(inputStream)
    val numRows = IntSerializer.read(inputStream)
    val numCols = IntSerializer.read(inputStream)
    val weightedVt = new DenseMatrix(numRows, numCols)

    val inputData = DoubleArraySerializer.read(inputStream)
    val toOverwrite = weightedVt.getData
    System.arraycopy(inputData, 0, toOverwrite, 0, inputData.length)
    new SVDTransform(k, weightedVt)
  }

}

object DimensionalityReductionTransformSerializer{
  def toBinary(outputStream: OutputStream, dimRedTransform: DimensionalityReductionTransform): Unit = {
    dimRedTransform match {
      case NoDimensionalityReductionTransform => {
        IntSerializer.write(outputStream, 0)
      }
      case svdTransform: SVDTransform => {
        IntSerializer.write(outputStream, 1)
        SVDTransformSerializer.toBinary(outputStream, svdTransform)
      }
    }
  }

  def fromBinary(inputStream: InputStream): DimensionalityReductionTransform = {
    val tag = IntSerializer.read(inputStream)
    tag match {
      case 0 => NoDimensionalityReductionTransform
      case 1 => SVDTransformSerializer.fromBinary(inputStream)
    }
  }
}

object SignatureVectorsSerializer{
  import ImplicitSerializers._

  def toBinary(outputStream: OutputStream, sigVectors: SignatureVectors): Unit = {
    val vectors = sigVectors.signatureVectors
    val len = vectors.length
    outputStream.writeInt(len)
    var i = 0
    while(i < len){
      SparseVectorSerializer.toBinary(outputStream, vectors(i))
      i += 1
    }
  }

  def fromBinary(inputStream: InputStream): SignatureVectors = {
    val len = inputStream.readInt()
    val vectors = Array.ofDim[SparseVector](len)
    var i = 0
    while(i < len){
      vectors(i) = SparseVectorSerializer.fromBinary(inputStream)
      i += 1
    }
    new SignatureVectors(vectors)
  }
}

object PointSignaturesSerializer{
  import ImplicitSerializers._

  def toBinary(outputStream: OutputStream, pointSignatures: PointSignatures): Unit = {
    val signatures = pointSignatures.pointSignatures
    val len = signatures.length
    outputStream.writeInt(len)
    var i = 0
    while(i < len){
      LongArraySerializer.write(outputStream, signatures(i))
      i += 1
    }
  }

  def fromBinary(inputStream: InputStream): PointSignatures = {
    val len = inputStream.readInt()
    val vectors = Array.ofDim[Array[Long]](len)
    var i = 0
    while(i < len){
      vectors(i) = LongArraySerializer.read(inputStream)
      i += 1
    }
    new PointSignatures(vectors)
  }
}

object SparseVectorSerializer{
  import ImplicitSerializers._
  def toBinary(outputStream: OutputStream, vec: SparseVector): Unit = {
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

  def fromBinary(inputStream: InputStream): SparseVector = {
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

    new SparseVector(dim, ids, values)
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

object RandomTreeSerializer{
  import ImplicitSerializers._
  def toBinary(outputStream: OutputStream, randomTree: RandomTree): Unit = {
    if (randomTree == null){
      outputStream.writeInt(1)
    }
    else {
      randomTree match {
        case EmptyLeaf => outputStream.writeInt(1)
        case leaf: RandomTreeLeaf => {
          outputStream.writeInt(2)
          outputStream.writeInt(leaf.leafId)
          outputStream.writeInt(leaf.count)
        }
        case RandomTreeNode(id: Int, projVector: AbstractProjectionVector, count: Int, means: Array[Double], children: Array[RandomTree]) => {
          outputStream.writeInt(3)
          outputStream.writeInt(id)
          ProjectionVectorSerializer.toBinary(outputStream, projVector)
          outputStream.writeInt(count)
          outputStream.writeDoubleArray(means)
          outputStream.writeInt(children.length)
          for (tree <- children) {
            RandomTreeSerializer.toBinary(outputStream, tree)
          }
        }
        case RandomTreeNodeRoot(projVector: AbstractProjectionVector, child: RandomTree) => {
          outputStream.writeInt(4)
          ProjectionVectorSerializer.toBinary(outputStream, projVector)
          RandomTreeSerializer.toBinary(outputStream, child)
        }
      }
    }
  }

  def fromBinary(inputStream: InputStream): RandomTree = {
    val nodeType = inputStream.readInt()
    nodeType match {
      case 1 => null //EmptyLeaf
      case 2 => {
        val leafId = inputStream.readInt()
        val count = inputStream.readInt()
        RandomTreeLeaf(leafId, count)
      }
      case 3 => {
        val id = inputStream.readInt()
        val vec = ProjectionVectorSerializer.fromBinary(inputStream)
        val count = inputStream.readInt()
        val means = inputStream.readDoubleArray()
        val numTrees = inputStream.readInt()
        val trees = Array.ofDim[RandomTree](numTrees)
        var i = 0
        while(i < numTrees){
          val tree = RandomTreeSerializer.fromBinary(inputStream)
          trees(i) = tree
          i += 1
        }
        RandomTreeNode(id, vec, count, means, trees)
      }
      case 4 => {
        val vec = ProjectionVectorSerializer.fromBinary(inputStream)
        val child = RandomTreeSerializer.fromBinary(inputStream)
        RandomTreeNodeRoot(vec, child)
      }
    }
  }
}

object RandomTreesSerializer{
  import ImplicitSerializers._
  def toBinary(outputStream: OutputStream, randomTrees: RandomTrees): Unit = {
    DimensionalityReductionTransformSerializer.toBinary(outputStream, randomTrees.dimReductionTransform)
    SignatureVectorsSerializer.toBinary(outputStream, randomTrees.signatureVecs)
    SplitStrategySerializer.toBinary(outputStream, randomTrees.datasetSplitStrategy)
    ColumnHeaderSerializer.toBinary(outputStream, randomTrees.header)
    outputStream.writeInt(randomTrees.trees.length)
    for(tree <- randomTrees.trees){
      RandomTreeSerializer.toBinary(outputStream, tree)
    }
  }

  def fromBinary(inputStream: InputStream, invIndex: IndexImpl): RandomTrees = {
    val dimRedTransform = DimensionalityReductionTransformSerializer.fromBinary(inputStream)
    val sigVectors = SignatureVectorsSerializer.fromBinary(inputStream)
    val splitStrategy = SplitStrategySerializer.fromBinary(inputStream)
    val header = ColumnHeaderSerializer.fromBinary(inputStream)

    val numTrees = inputStream.readInt()
    val trees = Array.ofDim[RandomTree](numTrees)
    var i = 0
    while(i < numTrees){
      val tree = RandomTreeSerializer.fromBinary(inputStream)
      trees(i) = tree
      i += 1
    }
    new RandomTrees(dimRedTransform, sigVectors, splitStrategy, header, invIndex, trees)
  }
}
object SplitStrategySerializer{
  import ImplicitSerializers._

  def toBinary(stream: OutputStream, splitStrategy: DatasetSplitStrategy): Unit = {
    val tag = splitStrategy match {
      case h: HadamardProjectionSplitStrategy => 0
      case d: DataInformedSplitStrategy => 1
      case n: NoSplitStrategy => 2
    }
    stream.writeInt(tag)
  }

  def fromBinary(stream: java.io.InputStream): DatasetSplitStrategy = {
    val id = stream.readInt()
    id match{
      case 0 => new HadamardProjectionSplitStrategy()
      case 1 => new DataInformedSplitStrategy()
      case 2 => new NoSplitStrategy()
    }
  }
}

object ColumnHeaderSerializer{
  import ImplicitSerializers._
  def toBinary(stream: OutputStream, header: ColumnHeader): Unit = {
    stream.writeInt(header.numCols)
  }

  def fromBinary(stream: java.io.InputStream): ColumnHeader = {
    val numCols = stream.readInt()
    new ColumnHeaderImpl(numCols, null, null, null, null)
  }
}