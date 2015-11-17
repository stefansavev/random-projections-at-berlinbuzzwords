package com.stefansavev.randomprojections.implementation

case class IndexSettings(maxPntsPerBucket: Int,
                    numTrees: Int,
                    maxDepth: Option[Int],
                    projectionStrategyBuilder: ProjectionStrategyBuilder,
                    reportingDistanceEvaluator: ReportingDistanceEvaluatorBuilder,
                    randomSeed: Int,
                    signatureSize: Int = 16
                    )

