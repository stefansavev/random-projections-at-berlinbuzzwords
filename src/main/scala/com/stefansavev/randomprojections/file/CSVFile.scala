package com.stefansavev.randomprojections.file

import scala.io.Source
import scala.util.matching.Regex

case class CSVFileOptions(sep: String = ",",
                          hasHeader: Boolean = true,
                          quote: Option[String] = None,
                          onlyTopRecords: Option[Int] = None)

class CSVFile private (_header: Option[Array[String]], source: Source, iter: Iterator[String],  opt: CSVFileOptions) {
  def numColumns: Option[Int] = _header.map(_.length)
  def header: Option[Array[String]] = _header

  def processLine(line: String): Array[String] = {
    CSVFile.processLine(opt, line) //TODO: verify num columns
  }

  def getLines(): Iterator[Array[String]] = iter.map(line => processLine(line))

  def close() = source.close()
}

object CSVFile {
  def processLine(opt: CSVFileOptions, line: String): Array[String] = {
    line.split(Regex.quote(opt.sep))
  }

  def read(fileName: String, opt: CSVFileOptions): CSVFile = {
    val source = Source.fromFile(fileName)
    val linesIterator = source.getLines()
    val iterator = opt.onlyTopRecords match {
      case None => linesIterator
      case Some(n) => linesIterator.take(n + 1) //1 is for the header
    }
    val header = if (opt.hasHeader) Some(processLine(opt, iterator.next())) else None
    new CSVFile(header, source, iterator, opt)
  }
}


