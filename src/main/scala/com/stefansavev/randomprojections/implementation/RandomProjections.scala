package com.stefansavev.randomprojections.implementation

object ProjectionStrategies {
  def splitIntoKRandomProjection(k: Int = 4): SplitIntoKProjectionBuilder = new SplitIntoKProjectionBuilder(SplitIntoKProjectionSettings(k))

  def dataInformedProjectionStrategy(): DataInformedProjectionBuilder = new DataInformedProjectionBuilder(DataInformedProjectionSettings())

  def onlySignaturesStrategy(): OnlySignaturesBuilder = new OnlySignaturesBuilder(OnlySignaturesSettings())
}