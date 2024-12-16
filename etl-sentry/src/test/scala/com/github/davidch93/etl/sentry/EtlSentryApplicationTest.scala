package com.github.davidch93.etl.sentry

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A template class to test various cases.
 *
 * @author david.christianto
 */
class EtlSentryApplicationTest extends AnyFunSuite with Matchers {

  test("Running a test should return Hello World") {
    val actual = "Hello World!"
    actual shouldEqual "Hello World!"
  }
}
