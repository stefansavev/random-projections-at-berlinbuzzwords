package com.stefansavev.randomprojections.serialization

import java.io._
import java.nio.ByteBuffer
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.datarepr.dense.{ColumnHeaderImpl, ColumnHeader}

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
    ColumnHeaderSerializer.toBinary(outputStream, randomTrees.header)
    outputStream.writeInt(randomTrees.trees.length)
    for(tree <- randomTrees.trees){
      RandomTreeSerializer.toBinary(outputStream, tree)
    }

  }

  def fromBinary(inputStream: InputStream, invIndex: IndexImpl): RandomTrees = {
    val header = ColumnHeaderSerializer.fromBinary(inputStream)

    val numTrees = inputStream.readInt()
    val trees = Array.ofDim[RandomTree](numTrees)
    var i = 0
    while(i < numTrees){
      val tree = RandomTreeSerializer.fromBinary(inputStream)
      trees(i) = tree
      i += 1
    }
    new RandomTrees(header, invIndex, trees)
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