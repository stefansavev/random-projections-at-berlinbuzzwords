package com.stefansavev.randomprojections.serialization

import java.io.File

import com.stefansavev.randomprojections.datarepr.dense.{ValuesStore, DataFrameView}
import com.stefansavev.randomprojections.serialization.DataFrameViewSerializers._
import com.stefansavev.randomprojections.serialization.core.Core

object ValueStoreSerializationExt {

  implicit class ValueStoreSerializerExt(input: ValuesStore){
    def toFile(file:File): Unit = {
      val ser = valuesStoreSerializer()
      Core.toFile(ser, file, input)
    }

    def toFile(fileName: String): Unit = {
      toFile(new File(fileName))
    }
  }

  implicit class ValueStoreDeserializerExt(t: ValuesStore.type){
    def fromFile(file:File): ValuesStore = {
      if (!file.exists()) {
        throw new IllegalStateException("file does not exist: " + file.getAbsolutePath)
      }
      println("Loading file: " + file.getAbsolutePath)
      val ser = valuesStoreSerializer()
      val output = Core.fromFile(ser, file)
      output
    }

    def fromFile(fileName: String): ValuesStore = {
      fromFile(new File(fileName))
    }
  }
}
