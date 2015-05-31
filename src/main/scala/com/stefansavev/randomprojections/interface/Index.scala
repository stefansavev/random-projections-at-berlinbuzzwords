package com.stefansavev.randomprojections.interface

import java.io.File

import com.stefansavev.randomprojections.implementation.{SearchBucketsResult, KNNS, SearcherSettings}
import com.stefansavev.randomprojections.implementation.bucketsearch.PointScoreSettings
import com.stefansavev.randomprojections.implementation.query.NearestNeigbhorQueryScratchBuffer

trait Index{
  def getNearestNeighbors(k: Int, pointName: Int, query: Array[Double], settings: SearcherSettings, searchBucketsResult: SearchBucketsResult, scratchBuffer: NearestNeigbhorQueryScratchBuffer): KNNS
  def toFile(file: File): Unit
}