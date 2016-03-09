package com.stefansavev.randomprojections.utils

object HashUtils {
  def charArrayHash(arr: Array[Char], fromPos: Int, toPos: Int): Int = {
    YonikMurmurHash3.murmurhash3_x86_32(arr, fromPos, toPos - fromPos, 93151)
  }
}
