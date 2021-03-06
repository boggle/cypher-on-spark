package org.opencypher.spark

import org.junit.runner.RunWith
import org.opencypher.spark.api.CypherImplicits
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
abstract class StdTestSuite
  extends FunSuite
  with Matchers
  with CypherImplicits

