package com.stefansavev.randomprojections.implementation

object ProjectionStrategies{
  def splitIntoKRandomProjection(k: Int): SplitIntoKProjectionBuilder = new SplitIntoKProjectionBuilder(SplitIntoKProjectionSettings(k))
  def dataInformedProjectionStrategy(): DataInformedProjectionBuilder = new DataInformedProjectionBuilder(DataInformedProjectionSettings())
}