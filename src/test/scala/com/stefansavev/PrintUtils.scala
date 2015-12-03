package com.stefansavev

import java.io.PrintWriter

import scala.io.{Codec, Source}

object PrintUtils {
  def columnVectorToFile(fileName: String, v: Array[Double]): Unit = {
    val writer = new PrintWriter(fileName)
    for(a <- v){
      writer.println(a.toString)
    }
    writer.close()
  }

  def stringsToFile(fileName: String, v: Array[String]): Unit = {
    val writer = new PrintWriter(fileName)
    for(a <- v){
      writer.println(a)
    }
    writer.close()
  }

  def withPrintWriter(fileName: String, body: PrintWriter => Unit): Unit = {
    val writer = new PrintWriter(fileName, "UTF-8")
    body(writer)
    writer.close()
  }

}

object FileReadUtils{
  def withLinesIterator[T](fileName: String)(body: Iterator[String] => T): T = {
    val source = Source.fromFile(fileName)(Codec.UTF8)
    val result = body(source.getLines())
    source.close()
    result
  }
}
