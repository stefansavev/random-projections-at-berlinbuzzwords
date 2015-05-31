package com.stefansavev.randomprojections.implementation

trait RandomTree{
  def getCount: Int
}

case object EmptyLeaf extends RandomTree{
  def getCount: Int = 0
}

case class RandomTreeLeaf(leafId: Int, count: Int) extends RandomTree{
  def getCount: Int = count
}


case class RandomTreeNodeRoot(projVector: AbstractProjectionVector, child: RandomTree) extends RandomTree{
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

case class RandomTreeNode(id: Int, projVector: AbstractProjectionVector, count: Int, means: Array[Double], children: Array[RandomTree]) extends RandomTree{
  def getCount: Int = count
}
