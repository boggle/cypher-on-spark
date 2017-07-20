/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark.impl.spark

import org.opencypher.spark.SparkCypherTestSuite
import org.opencypher.spark.api.expr.{Expr, Not, Var}
import org.opencypher.spark.api.ir.global.TokenRegistry
import org.opencypher.spark.api.spark.{SparkCypherRecords, SparkGraphSpace}
import org.opencypher.spark.api.types.{CTBoolean, CTInteger, CTString}

class SparkCypherEngineOperationsTest extends SparkCypherTestSuite {

  import operations._

  implicit val space = SparkGraphSpace.empty(session, TokenRegistry.empty)

  test("filter operation on records") {

    val given = SparkCypherRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1, true, "Mats"),
        (2, false, "Martin"),
        (3, false, "Max"),
        (4, false, "Stefan")
    ))

    val result = space.base.filter(given, Var("IS_SWEDE")(CTBoolean))

    result.toDF().count() should equal(1L)
  }

  test("select operation on records") {
    val given = SparkCypherRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1, true, "Mats"),
        (2, false, "Martin"),
        (3, false, "Max"),
        (4, false, "Stefan")
    ))

    val result = space.base.select(given, IndexedSeq(Var("ID")(CTInteger), Var("NAME")(CTString)))

    result.details shouldMatch SparkCypherRecords.create(
      Seq("ID", "NAME"), Seq(
        (1, "Mats"),
        (2, "Martin"),
        (3, "Max"),
        (4, "Stefan")
    ))
  }

  test("project operation on records") {
    val given = SparkCypherRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1, true, "Mats"),
        (2, false, "Martin"),
        (3, false, "Max"),
        (4, false, "Stefan")
      ))

    val expr: Expr = Not(Var("IS_SWEDE")(CTBoolean))(CTBoolean)
    val result = space.base.project(given, expr)

    result.details shouldMatchOpaquely SparkCypherRecords.create(
      Seq("ID", "IS_SWEDE", "NAME", "NOT IS_SWEDE"),
      Seq(
        (1, true, "Mats", false),
        (2, false, "Martin", true),
        (3, false, "Max", true),
        (4, false, "Stefan", true)
      ))
  }

  test("project operation with alias on records") {
    val given = SparkCypherRecords.create(
      Seq("ID", "IS_SWEDE", "NAME"),
      Seq(
        (1, true, "Mats"),
        (2, false, "Martin"),
        (3, false, "Max"),
        (4, false, "Stefan")
      ))

    val exprVar = Not(Var("IS_SWEDE")(CTBoolean))(CTBoolean) -> Var("IS_NOT_SWEDE")(CTBoolean)
    val result = space.base.alias(given, exprVar)

    result.details shouldMatchOpaquely SparkCypherRecords.create(
      Seq("ID", "IS_SWEDE", "NAME", "IS_NOT_SWEDE"),
      Seq(
        (1, true, "Mats", false),
        (2, false, "Martin", true),
        (3, false, "Max", true),
        (4, false, "Stefan", true)
      ))
  }
}
