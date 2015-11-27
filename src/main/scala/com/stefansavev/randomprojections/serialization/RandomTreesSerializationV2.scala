package com.stefansavev.randomprojections.serialization

import java.io.{InputStream, OutputStream}

import com.stefansavev.randomprojections.datarepr.dense.{DataFrameView}
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.dimensionalityreduction.interface.{NoDimensionalityReductionTransform, DimensionalityReductionTransform}
import com.stefansavev.randomprojections.dimensionalityreduction.svd.SVDTransform
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.serialization.core.PrimitiveTypeSerializers.TypedIntSerializer
import com.stefansavev.randomprojections.serialization.core.TypedSerializer
import no.uib.cipr.matrix.DenseMatrix

object RandomTreesSerializersV2 {

  object SVDTransformSerializer extends TypedSerializer[SVDTransform]{
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

    def sizeInBytes(svdTransform: SVDTransform): Long = {
      val matrix = svdTransform.weightedVt
      IntSerializer.sizeInBytes + //k
        IntSerializer.sizeInBytes*2 + //numrows, numCols
          DoubleArraySerializer.sizeInBytes(matrix.getData)
    }
  }

  implicit object DimensionalityReductionTransformSerializer extends TypedSerializer[DimensionalityReductionTransform]{
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

    def sizeInBytes(dimRedTransform: DimensionalityReductionTransform): Long = {
      dimRedTransform match {
        case NoDimensionalityReductionTransform => {
          IntSerializer.sizeInBytes
        }
        case svdTransform: SVDTransform => {
          IntSerializer.sizeInBytes
          SVDTransformSerializer.sizeInBytes(svdTransform)
        }
      }
    }
  }

  object ReportingDistanceEvaluatorSerializer extends TypedSerializer[ReportingDistanceEvaluator]{
    def toBinary(outputStream: OutputStream, distanceEvaluator: ReportingDistanceEvaluator): Unit = {
      distanceEvaluator match {
        case evaluator: CosineOnOriginalDataDistanceEvaluator => {
          IntSerializer.write(outputStream, 0)
        }
      }
    }

    def fromBinary(inputStream: InputStream): ReportingDistanceEvaluator = {
      val tag = IntSerializer.read(inputStream)
      tag match {
        case 0 => {
          val origDataset: DataFrameView = null
          new CosineOnOriginalDataDistanceEvaluator(origDataset)
        }
      }
    }

    def sizeInBytes(input: ReportingDistanceEvaluator): Long = {
      IntSerializer.sizeInBytes
    }
  }

  implicit object SignatureVectorsSerializer extends TypedSerializer[SignatureVectors]{
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

    def sizeInBytes(sigVectors: SignatureVectors): Long = {
      var sum = 0L
      val vectors = sigVectors.signatureVectors
      val len = vectors.length
      sum += IntSerializer.sizeInBytes
      var i = 0
      while(i < len){
        sum += SparseVectorSerializer.sizeInBytes(vectors(i))
        i += 1
      }
      sum
    }
  }

  implicit object SparseVectorSerializer extends TypedSerializer[SparseVector]{
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

    def sizeInBytes(vec: SparseVector): Long = {
      var sum = 0L
      sum += IntSerializer.sizeInBytes //dim
      sum += IntArraySerializer.sizeInBytes(vec.ids)
      sum += DoubleArraySerializer.sizeInBytes(vec.values)
      sum
    }
  }

  implicit object SplitStrategySerializer extends TypedSerializer[DatasetSplitStrategy]{
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

    def sizeInBytes(input: DatasetSplitStrategy): Long = {
      IntSerializer.sizeInBytes
    }
  }

  object RandomTreesSerializer extends TypedSerializer[RandomTrees]{
    import ImplicitSerializers._
    import ColumnHeaderSerialization._

    def toBinary(outputStream: OutputStream, randomTrees: RandomTrees): Unit = {
      DimensionalityReductionTransformSerializer.toBinary(outputStream, randomTrees.dimReductionTransform)
      ReportingDistanceEvaluatorSerializer.toBinary(outputStream, randomTrees.reportingDistanceEvaluator)
      SignatureVectorsSerializer.toBinary(outputStream, randomTrees.signatureVecs)
      SplitStrategySerializer.toBinary(outputStream, randomTrees.datasetSplitStrategy)
      ColumnHeaderSerializer.toBinary(outputStream, randomTrees.header)
      outputStream.writeInt(randomTrees.trees.length)
      for(tree <- randomTrees.trees){
        RandomTreeSerializer.toBinary(outputStream, tree)
      }
    }

    def fromBinary(inputStream: InputStream /*, invIndex: IndexImpl*/): RandomTrees = {
      val dimRedTransform = DimensionalityReductionTransformSerializer.fromBinary(inputStream)
      val distanceEvaluator = ReportingDistanceEvaluatorSerializer.fromBinary(inputStream)
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
      new RandomTrees(dimRedTransform, distanceEvaluator, sigVectors, splitStrategy, header, null/*must be set by the caller*/, trees)
    }

    def sizeInBytes(randomTrees: RandomTrees): Long = {
      var sum = 0L
      sum += DimensionalityReductionTransformSerializer.sizeInBytes(randomTrees.dimReductionTransform)
      sum += ReportingDistanceEvaluatorSerializer.sizeInBytes(randomTrees.reportingDistanceEvaluator)
      sum += SignatureVectorsSerializer.sizeInBytes(randomTrees.signatureVecs)
      sum += SplitStrategySerializer.sizeInBytes(randomTrees.datasetSplitStrategy)
      sum += ColumnHeaderSerializer.sizeInBytes(randomTrees.header)
      sum += TypedIntSerializer.sizeInBytes(randomTrees.trees.length)
      for(tree <- randomTrees.trees){
        //RandomTreeSerializer.toBinary(outputStream, tree)
      }
      sum
    }
  }


}
