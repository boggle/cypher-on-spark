package org.opencypher.spark

import org.scalatest.FunSuite

class CypherOnSparkPlannerTest extends FunSuite {

  test("foo") {
    val planner = new CypherOnSparkPlanner



    val result = planner.plan(null, SparkGraphImpl())


    result.show()

  }
  test("foo2") {
    val planner = new CypherOnSparkPlanner



    val result = planner.plan2(null, SparkGraphImpl())


    result.show()

  }
  test("foo3") {
    val planner = new CypherOnSparkPlanner



    val result = planner.plan3(null, SparkGraphImpl())


    result.show()

    println(result.rdd.toLocalIterator.toList)
//
//    val actualResults = result.map { v: CypherValue => v match {
//      case
//    }
//
//    }

  }

}
