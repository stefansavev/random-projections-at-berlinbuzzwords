package com.stefansavev.randomprojections.implementation

import java.util.Comparator

case class KNN(neighborId: Int, count: Int, var label: Int = -1, var dist: Double = 0.0)

case class KNNS(k: Int, pointId: Int, neighbors: Array[KNN])

class KNNDistanceComparator extends Comparator[KNN] {
  override def compare(o1: KNN, o2: KNN): Int = o2.dist.compareTo(o1.dist)
}