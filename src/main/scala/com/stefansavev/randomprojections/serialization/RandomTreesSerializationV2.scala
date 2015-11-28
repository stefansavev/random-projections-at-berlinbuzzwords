package com.stefansavev.randomprojections.serialization

import java.io.{InputStream, OutputStream}

import com.stefansavev.randomprojections.datarepr.dense.{DataFrameView}
import com.stefansavev.randomprojections.datarepr.sparse.SparseVector
import com.stefansavev.randomprojections.dimensionalityreduction.interface.{NoDimensionalityReductionTransform, DimensionalityReductionTransform}
import com.stefansavev.randomprojections.dimensionalityreduction.svd.SVDTransform
import com.stefansavev.randomprojections.implementation._
import com.stefansavev.randomprojections.serialization.core.PrimitiveTypeSerializers.{TypedIntArraySerializer, TypedDoubleArraySerializer, TypedIntSerializer}
import com.stefansavev.randomprojections.serialization.core.{MemoryTrackingUtils, MemoryTracker, TypedSerializer}
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

    def sizeInBytes(memoryTracker: MemoryTracker, svdTransform: SVDTransform): Long = {
      val matrix = svdTransform.weightedVt
      TypedIntSerializer.sizeInBytes(memoryTracker, svdTransform.k) + //k
        TypedIntSerializer.sizeInBytes(memoryTracker, matrix.numRows()) +
        TypedIntSerializer.sizeInBytes(memoryTracker, matrix.numColumns()) +
        TypedDoubleArraySerializer.sizeInBytes(memoryTracker, matrix.getData)
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

    def sizeInBytes(memoryTracker: MemoryTracker, dimRedTransform: DimensionalityReductionTransform): Long = {
      MemoryTrackingUtils.withNestingInfo(memoryTracker, dimRedTransform, {
        dimRedTransform match {
          case NoDimensionalityReductionTransform => {
            TypedIntSerializer.sizeInBytes(memoryTracker, 0)
          }
          case svdTransform: SVDTransform => {
            TypedIntSerializer.sizeInBytes(memoryTracker, 1) +
              SVDTransformSerializer.sizeInBytes(memoryTracker, svdTransform)
          }
        }
      })
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

    def sizeInBytes(memoryTracker: MemoryTracker, input: ReportingDistanceEvaluator): Long = {
      MemoryTrackingUtils.withNestingInfo(memoryTracker, input, {
        TypedIntSerializer.sizeInBytes(memoryTracker, 0)
      })
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

    def sizeInBytes(memoryTracker: MemoryTracker, sigVectors: SignatureVectors): Long = {
      MemoryTrackingUtils.withNestingInfo(memoryTracker, sigVectors, {
        var sum = 0L
        val vectors = sigVectors.signatureVectors
        val len = vectors.length
        sum += TypedIntSerializer.sizeInBytes(memoryTracker, len)
        var i = 0
        while (i < len) {
          sum += SparseVectorSerializer.sizeInBytes(memoryTracker, vectors(i))
          i += 1
        }
        sum
      })
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

    def sizeInBytes(memoryTracker: MemoryTracker, vec: SparseVector): Long = {
      MemoryTrackingUtils.withNestingInfo(memoryTracker, vec, {
        var sum = 0L
        sum += TypedIntSerializer.sizeInBytes(memoryTracker, vec.dim) //dim
        sum += TypedIntArraySerializer.sizeInBytes(memoryTracker, vec.ids)
        sum += TypedDoubleArraySerializer.sizeInBytes(memoryTracker, vec.values)
        sum
      })
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

    def sizeInBytes(memoryTracker: MemoryTracker, input: DatasetSplitStrategy): Long = {
      IntSerializer.sizeInBytes
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

    def sizeInBytes(memoryTracker: MemoryTracker, randomTree: RandomTree): Long = {
      MemoryTrackingUtils.withGenericImplementation(memoryTracker, this, randomTree)
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

    def sizeInBytes(memoryTracker: MemoryTracker, randomTrees: RandomTrees): Long = {
      MemoryTrackingUtils.withNestingInfo(memoryTracker, randomTrees, {
        var sum = 0L
        sum += DimensionalityReductionTransformSerializer.sizeInBytes(memoryTracker, randomTrees.dimReductionTransform)
        sum += ReportingDistanceEvaluatorSerializer.sizeInBytes(memoryTracker, randomTrees.reportingDistanceEvaluator)
        sum += SignatureVectorsSerializer.sizeInBytes(memoryTracker, randomTrees.signatureVecs)
        sum += SplitStrategySerializer.sizeInBytes(memoryTracker, randomTrees.datasetSplitStrategy)
        sum += ColumnHeaderSerializer.sizeInBytes(memoryTracker, randomTrees.header)
        sum += TypedIntSerializer.sizeInBytes(memoryTracker, randomTrees.trees.length)
        for (tree <- randomTrees.trees) {
          sum += RandomTreeSerializer.sizeInBytes(memoryTracker, tree)
        }
        sum
      })
    }
  }


}
