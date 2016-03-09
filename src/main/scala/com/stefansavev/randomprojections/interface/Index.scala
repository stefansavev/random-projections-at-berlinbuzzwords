package com.stefansavev.randomprojections.interface

import java.io.File

import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer
import com.stefansavev.randomprojections.implementation.{KNNS, SearchBucketsResult, SearcherSettings}

trait Index {
  def getNearestNeighbors(k: Int, pointName: Int, query: Array[Double], settings: SearcherSettings, searchBucketsResult: SearchBucketsResult, scratchBuffer: NearestNeigbhorQueryScratchBuffer): KNNS

  def toFile(file: File): Unit
}