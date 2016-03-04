package com.stefansavev.core.string2id


object TestDynamicString2IdHasher{
  def main (args: Array[String]): Unit = {
    val table = new String2UniqueIdTable()
    def makeName(i: Int): String = i + "#" + i
    for(i <- 0 until 10000000){
      val result = table.addString(makeName(i)).stringId
      //println("added " + i + " at " + result)
    }
    for(i <- 0 until 10000000){
      val str = table.getStringById(i)
      if (str != makeName(i).toString){
        throw new IllegalStateException("problem")
      }
      //println("got " + str + " " + i)

    }
  }
}

object String2IdHasherTester{
  def main (args: Array[String]): Unit = {
    val settings = StringIdHasherSettings(2/*8*/, 50, 4)
    val h = new String2IdHasher(settings)
    val input = Array("a", "a", "b", "cd", "p", "q")
    for(word <- input){
      println("adding " + word)
      val arr = word.toCharArray
      val index = h.getOrAddId(arr, 0, arr.length, true)
      if (index >= 0) {
        println("added: " + h.getStringAtIndex(index) + " at index " + index)
      }
      else{
        println("cannot add " + word)
      }
    }
    println("dump contents")
    h.dumpContents()
  }
}

