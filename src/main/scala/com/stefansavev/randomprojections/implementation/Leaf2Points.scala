package com.stefansavev.randomprojections.implementation

import com.stefansavev.randomprojections.utils.Utils

class Leaf2Points(val starts: Array[Int], val points: Array[Int]) {
  //points are numbered from 0
  def getPointCountInBucket(offset: Int): Int = Utils.todo()

  def getTotalPointCount(pointId: Int): Int = Utils.todo()

  def numberOfLeaves(): Int = starts.length - 1
}
