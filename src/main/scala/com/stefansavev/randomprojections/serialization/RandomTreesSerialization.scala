package com.stefansavev.randomprojections.serialization

import java.io._
import java.nio.ByteBuffer

import com.stefansavev.randomprojections.implementation.{RandomTrees, IndexImpl, Leaf2Points}
import com.stefansavev.randomprojections.serialization.RandomTreesSerialization.BinaryFileDeserializer

import scala.collection.mutable.ArrayBuffer

object InvertedIndexSerializer{
  def fromFile(file: File): IndexImpl = {

    val deser = new BinaryFileDeserializer(file)

    val signatures = PointSignaturesSerializer.fromBinary(deser.stream)
    val leafArrays = deser.getIntArrays(2)
    val numLeaves = deser.getInt()

    val numPoints = deser.getInt()

    val labels = deser.getIntArray()

    val index = new IndexImpl(signatures, numPoints, new Leaf2Points(leafArrays(0), leafArrays(1)),
      /*new Point2Leaves(pointArrays(0), pointArrays(1), numPoints),*/ labels)

    deser.close()
    index
  }
}



object RandomTreesSerialization{

  class BinaryFileDeserializer(file: File){
    val stream = new BufferedInputStream(new FileInputStream(file))
    val bytes = Array.ofDim[Byte](4)
    val b = ByteBuffer.allocate(4)
    val _isValid = readIsValidSignature()


    def readIsValidSignature(): Boolean = {
      try {
        BinaryFileSerializerSig.isValidSignature(getIntArray())
      }
      catch{
        case e: IOException => false
      }
    }

    def isValid(): Boolean = _isValid

    def getInt(): Int = {
      IntSerializer.read(stream)
    }

    def getIntArray(): Array[Int] = {
      val len = getInt()
      val output = Array.ofDim[Int](len)

      var i = 0

      while(i < len){
        output(i) = getInt()
        i += 1
      }
      output
    }

    def getIntArrays(k: Int): Array[Array[Int]] = {
      val buf = new ArrayBuffer[Array[Int]]()
      for(i <- 0 until k){
        buf += getIntArray()

      }
      buf.toArray
    }

    def close(): Unit = {
      stream.close()
    }
  }

  object Implicits {
    val modelFileName = "model.bin"
    val indexFileName = "index.bin"

    implicit class RandomTreesSerExt(trees: RandomTrees){

      def toFile(file:File): Unit = {
        if (!file.exists()) {
          file.mkdir()
        }
        else{
          if (!file.isDirectory) {
            throw new IllegalStateException("File " + file + " should be directory")
          }
        }

        val indexFile = new File(file, indexFileName)

        if (indexFile.exists()){
          val deser = new BinaryFileDeserializer(indexFile) //maybe the file cannot be opened
          val isValid = deser.isValid()
          deser.close()
          if (isValid){
            if (!indexFile.delete()){
              throw new IllegalStateException("old index file cannot be deleted" + indexFile.getAbsolutePath)
            }
          }
          else{
            throw new IllegalStateException("unrecognized file cannot be deleted: " + indexFile.getAbsolutePath)
          }
        }
        trees.invertedIndex.toFile(indexFile)
        val modelFile = new File(file, modelFileName)

        val bos = new BufferedOutputStream(new FileOutputStream(modelFile))
        RandomTreesSerializer.toBinary(bos, trees)
        bos.close()
      }

      def toFile(fileName: String): Unit = {
        toFile(new File(fileName))
      }
    }

    implicit class RandomTreesDeserExt(t: RandomTrees.type){

      def fromFile(file:File): RandomTrees = {
        if (!file.exists()) {
          throw new IllegalStateException("file does not exist")
        }

        if (!file.isDirectory()){
          throw new IllegalStateException("File is not a directory")
        }
        val modelFile = new File(file, modelFileName)
        val invIndexFile = new File(file, indexFileName)

        val invIndex = InvertedIndexSerializer.fromFile(invIndexFile)
        val bos = new BufferedInputStream(new FileInputStream(modelFile))
        val trees = RandomTreesSerializer.fromBinary(bos, invIndex)
        bos.close()
        trees
      }

      def fromFile(dir: String): RandomTrees = {
        fromFile(new File(dir))
      }
    }


  }

}


