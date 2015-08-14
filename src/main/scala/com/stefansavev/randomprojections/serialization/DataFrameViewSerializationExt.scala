package com.stefansavev.randomprojections.serialization

import java.io.File
import com.stefansavev.randomprojections.datarepr.dense.DataFrameView
import com.stefansavev.randomprojections.serialization.core.Core
import DataFrameViewSerializers.dataFrameSerializer

object DataFrameViewSerializationExt {

  implicit class DataFrameSerializerExt(input: DataFrameView){
    def toFile(file:File): Unit = {
      val ser = dataFrameSerializer()
      Core.toFile(ser, file, input)
    }

    def toFile(fileName: String): Unit = {
      toFile(new File(fileName))
    }
  }

  implicit class DataFrameDeserializerExt(t: DataFrameView.type){

    def fromFile(file:File): DataFrameView = {
      if (!file.exists()) {
        throw new IllegalStateException("file does not exist")
      }

      val ser = dataFrameSerializer()
      val output = Core.fromFile(ser, file)
      output
    }

    def fromFile(dir: String): DataFrameView = {
      fromFile(new File(dir))
    }
  }
}
