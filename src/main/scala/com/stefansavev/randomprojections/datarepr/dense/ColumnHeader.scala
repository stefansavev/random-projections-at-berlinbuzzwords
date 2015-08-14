package com.stefansavev.randomprojections.datarepr.dense
import scala.collection.mutable

trait ColumnHeader{
  def numCols: Int
  def labelName: String
  def getId2ColumnName(id: Int): Option[String]
  def getColumnName2Id(name: String): Option[Int]
  def getRowNames: Array[String]
  def isCompatible(header: ColumnHeader): Boolean
}

class ColumnHeaderImpl(_numCols: Int,
                       _labelName: String,
                       id2ColumnNameMap: mutable.HashMap[Int, String],
                       name2ColumnNameMap: mutable.HashMap[String, Int],
                       rowNames: Array[String]) extends ColumnHeader{
  override def numCols: Int = _numCols
  override def labelName: String = _labelName
  override def getId2ColumnName(id: Int): Option[String] = id2ColumnNameMap.get(id)
  override def getColumnName2Id(name: String): Option[Int] = name2ColumnNameMap.get(name)
  override def getRowNames: Array[String] = rowNames

  override def isCompatible(header: ColumnHeader): Boolean = {
    if (numCols == header.numCols){ // && labelName == header.labelName){
      //TODO: check column names
      true
    }
    else{
      false
    }
  }

}

object ColumnHeaderBuilder{
  def build(_labelName: String, columnIds: Array[(String, Int)], rowNames: Array[String]): ColumnHeader = {
    val id2ColumnNameMap = mutable.HashMap[Int, String]()

    val name2ColumnNameMap = mutable.HashMap[String, Int]()

    def addOrFail[K, V](h: mutable.HashMap[K, V], k: K, v: V, msg: String): Unit = {
      if (h.contains(k)){
        throw new IllegalStateException("Duplicate " + msg + k.toString + " detected")
      }
      h.put(k, v)
    }

    for((name, id) <- columnIds){
      addOrFail(id2ColumnNameMap, id, name, "id")
      addOrFail(name2ColumnNameMap, name, id, "name")
    }

    new ColumnHeaderImpl(columnIds.length, _labelName, id2ColumnNameMap, name2ColumnNameMap,rowNames)
  }
}