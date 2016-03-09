package com.stefansavev.randomprojections.implementation

import com.stefansavev.randomprojections.datarepr.sparse.BinarySparseVector8
import com.stefansavev.randomprojections.utils.Utils

trait RandomTree {
  def getCount: Int
}

case object EmptyLeaf extends RandomTree {
  def getCount: Int = 0
}

case class RandomTreeLeaf(leafId: Int, count: Int) extends RandomTree {
  def getCount: Int = count
}

case class RandomTreeNodeRoot(projVector: AbstractProjectionVector, child: RandomTree) extends RandomTree {
  def modifyQuery(query: Array[Double]): Array[Double] = {
    val sparseVec = projVector.asInstanceOf[HadamardProjectionVector].signs
    val input = Array.ofDim[Double](sparseVec.ids.length) //TODO: REMOVE

    var i = 0
    while (i < sparseVec.ids.length) {
      val columnId = sparseVec.ids(i)
      input(i) = sparseVec.values(i) * query(columnId)
      i += 1
    }

    val output = Array.ofDim[Double](sparseVec.ids.length) //TODO
    HadamardUtils.multiplyInto(input, output)
    output
  }

  def getCount: Int = child.getCount
}

case class RandomTreeNode(id: Int, projVector: AbstractProjectionVector, count: Int, means: Array[Double], children: Array[RandomTree]) extends RandomTree {
  def getCount: Int = count
}

class MutableTreeLeaf {
  var leafId: Int = 0
  var count: Int = 0
}

class MutableTreeNonLeaf {
  var childArray: Array[Int] = null
  var proj: Array[Byte] = null
}

class TreeReadBuffer(values: Array[Int]) {

  def getInternalStore(): Array[Int] = values

  var pntr = 0

  def position(pos: Int): Unit = {
    pntr = pos / 4
  }

  def position(): Int = {
    val p2 = 4 * pntr
    p2
  }

  def getInt(): Int = {
    val tmp = values(pntr)
    pntr += 1
    tmp
  }

  def get8Bytes(output: Array[Byte]): Unit = {
    var i = 0
    var j = 0
    while (i < 2) {
      val intValue = values(pntr + i)
      output(j + 0) = intValue.toByte
      output(j + 1) = (intValue >>> 8).toByte
      output(j + 2) = (intValue >>> 16).toByte
      output(j + 3) = (intValue >>> 24).toByte
      i += 1
      j += 4
    }
    pntr += 2
  }

  def get(arr: Array[Byte]): Unit = {
    if (arr.length != 8) {
      Utils.internalError()
    }
    get8Bytes(arr)
  }
}


class TreeWriteBuffer(initialSize: Int) {
  val adjustedInitialSize = HadamardUtils.largestPowerOf2(initialSize)
  var output = Array.ofDim[Int](adjustedInitialSize)

  var pntr = 0

  def position(pos: Int): Unit = {
    pntr = pos / 4
  }

  def position(): Int = {
    4 * pntr
  }

  def ensureAvailableSpace(numNewValues: Int): Unit = {
    val expectedSize = pntr + numNewValues
    if (expectedSize >= output.length) {
      var nextSize = output.length << 1
      while (nextSize < expectedSize) {
        nextSize <<= 1
      }
      val newOutput = Array.ofDim[Int](nextSize)
      System.arraycopy(output, 0, newOutput, 0, pntr)
      output = newOutput
    }
  }

  def putInt(value: Int): Unit = {
    ensureAvailableSpace(1)
    output(pntr) = value
    pntr += 1
  }

  def put(arr: Array[Byte]): Unit = {
    ensureAvailableSpace(2)
    val combined1 = (arr(3) << 24) | ((arr(2) & 0xff) << 16) | ((arr(1) & 0xff) << 8) | (arr(0) & 0xff)
    output(pntr) = combined1
    val combined2 = (arr(7) << 24) | ((arr(6) & 0xff) << 16) | ((arr(5) & 0xff) << 8) | (arr(4) & 0xff)
    output(pntr + 1) = combined2
    pntr += 2
  }

  def getInternalRepr(): Array[Int] = {
    val size = pntr
    val origin = output
    val truncated = Array.ofDim[Int](size)
    System.arraycopy(origin, 0, truncated, 0, size)
    truncated
  }
}

class TreeReader(bytes: TreeReadBuffer) {
  def getInternalStore(): Array[Int] = bytes.getInternalStore()

  def buildMutableTreeLeaf(): MutableTreeLeaf = {
    new MutableTreeLeaf()
  }

  def buildMutableNonLeaf(): MutableTreeNonLeaf = {
    val maxNumChildren = 16
    val bytesPerProj = 8

    val nonLeaf = new MutableTreeNonLeaf()
    nonLeaf.childArray = Array.ofDim[Int](maxNumChildren)
    nonLeaf.proj = Array.ofDim[Byte](bytesPerProj)
    nonLeaf
  }

  def isLeaf(ref: Int): Boolean = {
    bytes.position(ref)
    val len = bytes.getInt()
    (len < 0)
  }

  def decodeLeaf(ref: Int, leaf: MutableTreeLeaf): MutableTreeLeaf = {
    bytes.position(ref)
    val len = bytes.getInt()
    if (len < 0) {
      val count = bytes.getInt()
      leaf.leafId = -len - 1
      leaf.count = count
      leaf
    }
    else {
      Utils.internalError()
    }
  }

  def decodeNonLeaf(ref: Int, output: MutableTreeNonLeaf): MutableTreeNonLeaf = {
    bytes.position(ref)
    val len = bytes.getInt()
    if (len < 0) {
      throw new IllegalStateException()
    }
    else {
      var mask: Int = bytes.getInt() & 0xFFFF
      bytes.get(output.proj)
      val childArray = output.childArray
      var posNextChildNode = bytes.position()
      var i = 0
      while (mask != 0) {
        if ((mask & 1) == 1) {
          childArray(i) = (posNextChildNode)
          bytes.position(posNextChildNode)
          val childNodeLen = bytes.getInt()
          val adjustedLen = if (childNodeLen < 0) 8 else childNodeLen //we store the leaf id and count (count is redundant)
          posNextChildNode += adjustedLen
        }
        else {
          childArray(i) = -1
        }
        mask >>>= 1
        i += 1
      }
      while (i < 16) {
        childArray(i) = -1
        i += 1
      }
    }
    output
  }
}

class EfficientlyStoredNodeBuilder(val pos: Int, treeBuilder: EfficientlyStoredTreeBuilder) {
  var childrenMask = 0

  def finishNode(): Unit = {
    treeBuilder.finishBlock(pos, childrenMask.toShort)
  }

  def addedChild(index: Int): Unit = {
    if (index >= 16) {
      Utils.internalError()
    }
    val currentMask = (1 << index)
    childrenMask |= currentMask
  }
}

class EfficientlyStoredTreeBuilder {
  val bytes = new TreeWriteBuffer(1024 * 100)

  def putRecord(proj: Array[Byte]): Int = {
    /*proj must be an array of length 8*/
    if (proj.length != 8) {
      throw new IllegalStateException("proj must have 8 bytes")
    }
    val pos = bytes.position()
    bytes.putInt(0) //reserved to be filled when the node is finished
    bytes.putInt(0) //reserved to be filled when the node is finished 2 bytes
    bytes.put(proj) //8 bytes
    pos
  }

  def finishBlock(startPos: Int, mask: Short): Unit = {
    val currentPos = bytes.position()
    bytes.position(startPos)
    bytes.putInt(currentPos - startPos)
    bytes.putInt(mask & 0xFFFF)
    bytes.position(currentPos)
  }

  def putLeaf(leafId: Int, count: Int): Unit = {
    //TODO: count does not need to be stored twice
    val asNegIndex = -leafId - 1
    bytes.putInt(asNegIndex)
    bytes.putInt(count)
  }

  def build(): Array[Int] = {
    bytes.getInternalRepr()
  }

  def newRandomTreeNode(binarySparseVector8: BinarySparseVector8): EfficientlyStoredNodeBuilder = {
    val projBytes = binarySparseVector8.ids
    val currentPos = putRecord(projBytes)
    new EfficientlyStoredNodeBuilder(currentPos, this)
  }
}

case class EfficientlyStoredTree(val treeReader: TreeReader) extends RandomTree {
  def getCount: Int = Utils.internalError()
}

object RandomTree2EfficientlyStoredTreeConverter {

  def roundTrip(tree: RandomTree): RandomTree = {
    val internalRepr = fromRandomTree(tree)
    toRandomTree(internalRepr)
  }

  def toEfficientlyStoredTree(tree: RandomTree): EfficientlyStoredTree = {
    val internalRepr = fromRandomTree(tree)
    val reader = new TreeReader(new TreeReadBuffer(internalRepr))
    EfficientlyStoredTree(reader)
  }

  def fromRandomTree(tree: RandomTree): Array[Int] = {
    val builder = new EfficientlyStoredTreeBuilder()
    def loop(tree: RandomTree): Boolean = {
      tree match {
        case rtn: RandomTreeNode => {
          val sv = rtn.projVector.asInstanceOf[HadamardProjectionVector].signs
          val binarySV8 = BinarySparseVector8.fromSparseVec(sv)
          val childNodeBuilder = builder.newRandomTreeNode(binarySV8)
          for ((child, index) <- rtn.children.zipWithIndex) {
            if (child != null) {
              if (loop(child)) {
                childNodeBuilder.addedChild(index)
              }
            }
          }
          childNodeBuilder.finishNode()
          true
        }
        case rtl: RandomTreeLeaf => {
          builder.putLeaf(rtl.leafId, rtl.count)
          true
        }
        case empty@EmptyLeaf => {
          false
        }
      }
    }
    loop(tree)
    builder.build()
  }

  def toRandomTree(ints: Array[Int]): RandomTree = {
    val reader = new TreeReader(new TreeReadBuffer(ints))

    def recurse(nodeRef: Int): RandomTree = {
      if (reader.isLeaf(nodeRef)) {
        val leafNode = reader.buildMutableTreeLeaf()
        reader.decodeLeaf(nodeRef, leafNode)
        RandomTreeLeaf(leafNode.leafId, leafNode.count)
      }
      else {
        val nonLeaf = reader.buildMutableNonLeaf()
        reader.decodeNonLeaf(nodeRef, nonLeaf)
        val proj = HadamardProjectionVector((new BinarySparseVector8(nonLeaf.proj)).toNormalizedSparseVector())
        val children = nonLeaf.childArray
        val len = children.length
        val randomTreeChildren = Array.ofDim[RandomTree](len)
        var i = 0
        while (i < nonLeaf.childArray.length) {
          val childRef = nonLeaf.childArray(i)
          if (childRef >= 0) {
            randomTreeChildren(i) = recurse(childRef)
          }
          i += 1
        }
        new RandomTreeNode(id = -1 /*not kept*/ , projVector = proj, count = -1 /*not kept*/ , Array(), randomTreeChildren)
      }
    }
    recurse(0)
  }
}