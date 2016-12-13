package com.stefansavev.core.string2id

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}


@RunWith(classOf[JUnitRunner])
class TestDynamicString2IdHasher extends FlatSpec with Matchers {
  val table = new String2UniqueIdTable()

  def makeName(i: Int): String = i + "#" + i

  for (i <- 0 until 100000) {
    table.addString(makeName(i))
  }

  "strings " should "be from 0 to 100000" in {
    for (i <- 0 until 100000) {
      val str = table.getStringById(i)
      val expectedStr = makeName(i).toString
      str should be(expectedStr)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class String2IdHasherTester extends FlatSpec with Matchers {
  "some strings" should "not be added because of lack of space" in {
    val settings = StringIdHasherSettings(2, 50, 4)
    val h = new String2IdHasher(settings)
    val input = Array("a", "a", "b", "cd", "p", "q")
    for (word <- input) {
      val arr = word.toCharArray
      val index = h.getOrAddId(arr, 0, arr.length, true)
      if (word == "cd" || word == "p" || word == "q") {
        //cannot add
        index should be(-2)
      }
    }
  }
}

