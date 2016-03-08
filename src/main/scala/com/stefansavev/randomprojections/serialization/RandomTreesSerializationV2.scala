package com.stefansavev.randomprojections.serialization

import java.io.{InputStream, OutputStream}

import com.stefansavev.core.serialization._
import com.stefansavev.core.serialization.core.Utils._
import com.stefansavev.core.serialization.core.PrimitiveTypeSerializers._
import com.stefansavev.core.serialization.core.TupleSerializers._
import com.stefansavev.core.serialization.core.IsoSerializers._
import com.stefansavev.core.serialization.core._
import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.dimensionalityreduction.interface.{DimensionalityReductionTransform, NoDimensionalityReductionTransform}
import com.stefansavev.randomprojections.dimensionalityreduction.svd.SVDTransform
import com.stefansavev.randomprojections.implementation._
import no.uib.cipr.matrix.DenseMatrix

object RandomTreesSerializersV2 {

  implicit def pointSignatureReferenceSerializer(): TypedSerializer[PointSignatureReference] = {

    implicit object PointSignatureReferenceIso extends Iso[PointSignatureReference, PointSignatureReference.TupleType]{
      def from(input: Input): Output = input.toTuple
      def to(t: Output): Input = PointSignatureReference.fromTuple(t)
    }

    implicit def pointSignatureReferenceTupleTypeSerializer(): TypedSerializer[PointSignatureReference.TupleType] = {
      tuple6Serializer[String, Int, Int, Int, Int, Array[Long]](TypedStringSerializer, TypedIntSerializer, TypedIntSerializer, TypedIntSerializer, TypedIntSerializer, TypedLongArraySerializer)
    }

    isoSerializer[PointSignatureReference, PointSignatureReference.TupleType](PointSignatureReferenceIso, pointSignatureReferenceTupleTypeSerializer())
  }

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
  }

  object RandomTreeSerializer extends TypedSerializer[RandomTree]{
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
          case EfficientlyStoredTree(treeReader: TreeReader) => {
            outputStream.writeInt(5)
            IntArraySerializer.write(outputStream, treeReader.getInternalStore())
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
        case 5 => {
          val internalStorage = IntArraySerializer.read(inputStream)
          EfficientlyStoredTree(new TreeReader(new TreeReadBuffer(internalStorage)))
        }
      }
    }
  }

  object RandomTreesSerializer extends TypedSerializer[RandomTrees]{
    import ColumnHeaderSerialization._
    import ImplicitSerializers._

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
  }


}
