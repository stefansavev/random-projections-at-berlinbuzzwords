package com.stefansavev

import org.junit.rules.TemporaryFolder
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.scalatest.{SuiteMixin, Outcome, Suite}

//Reference: http://stackoverflow.com/questions/32160549/using-junit-rule-with-scalatest-e-g-temporaryfolder
trait TemporaryFolderFixture extends SuiteMixin {
  this: Suite =>
  val temporaryFolder = new TemporaryFolder

  abstract override def withFixture(test: NoArgTest) = {
    var outcome: Outcome = null
    val statementBody = () => outcome = super.withFixture(test)
    temporaryFolder(
      new Statement() {
        override def evaluate(): Unit = statementBody()
      },
      Description.createSuiteDescription("JUnit rule wrapper")
    ).evaluate()
    outcome
  }
}
