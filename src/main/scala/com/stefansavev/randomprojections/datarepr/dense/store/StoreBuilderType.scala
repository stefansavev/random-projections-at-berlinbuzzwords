package com.stefansavev.randomprojections.datarepr.dense.store

import com.stefansavev.randomprojections.utils.Utils

trait StoreBuilderType {
  def getBuilder(numCols: Int): ValuesStoreBuilder
}

case object StoreBuilderAsDoubleType extends StoreBuilderType {
  def getBuilder(numCols: Int): ValuesStoreBuilder = new ValuesStoreBuilderAsDouble(numCols)
}

case object StoreBuilderAsBytesType extends StoreBuilderType {
  def getBuilder(numCols: Int): ValuesStoreBuilder = new ValuesStoreBuilderAsBytes(numCols)
}

case object StoreBuilderAsSingleByteType extends StoreBuilderType {
  def getBuilder(numCols: Int): ValuesStoreBuilder = new ValuesStoreBuilderAsSingleByte(numCols)
}

case class LazyLoadStoreBuilderType(backingDir: String, underlyingBuilder: StoreBuilderType) extends StoreBuilderType {
  if (underlyingBuilder.isInstanceOf[LazyLoadStoreBuilderType]) {
    Utils.internalError()
  }

  def getBuilder(numCols: Int): ValuesStoreBuilder = new LazyLoadValuesStoreBuilder(backingDir, numCols, underlyingBuilder)
}

case class AsyncStoreBuilderType(backingDir: String, underlyingBuilder: StoreBuilderType) extends StoreBuilderType {
  if (underlyingBuilder.isInstanceOf[LazyLoadStoreBuilderType]) {
    Utils.internalError()
  }

  def getBuilder(numCols: Int): ValuesStoreBuilder = new AsyncStoreBuilder(backingDir, numCols, underlyingBuilder)
}

