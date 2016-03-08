package com.stefansavev.randomprojections.serialization

import java.io.File

import com.stefansavev.core.serialization.core.Utils
import com.stefansavev.randomprojections.datarepr.dense.store.ValuesStore
import com.stefansavev.randomprojections.serialization.DataFrameViewSerializers._

object ValueStoreSerializationExt {
  val ser = valuesStoreSerializer()

  implicit class ValueStoreSerializerExt(input: ValuesStore){
    def toFile(file:File): Unit = {
      Utils.toFile(ser, file, input)
    }

    def toFile(fileName: String): Unit = {
      toFile(new File(fileName))
    }

    def toBytes(): Array[Byte] = {
      Utils.toBytes(ser, input)
    }
  }

  implicit class ValueStoreDeserializerExt(t: ValuesStore.type){
    def fromFile(file:File): ValuesStore = {
      if (!file.exists()) {
        throw new IllegalStateException("file does not exist: " + file.getAbsolutePath)
      }
      println("Loading file: " + file.getAbsolutePath)
      val output = Utils.fromFile(ser, file)
      output
    }

    def fromFile(fileName: String): ValuesStore = {
      fromFile(new File(fileName))
    }

    def fromBytes(input: Array[Byte]): ValuesStore = {
      Utils.fromBytes(ser, input)
    }
  }
}
