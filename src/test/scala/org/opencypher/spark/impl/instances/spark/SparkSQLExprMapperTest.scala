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
package org.opencypher.spark.impl.instances.spark

import java.util.Collections

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.opencypher.spark.api.expr.{Expr, Subtract, Var}
import org.opencypher.spark.api.record.{OpaqueField, ProjectedField, RecordHeader}
import org.opencypher.spark.impl.instances.spark.SparkSQLExprMapper.asSparkSQLExpr
import org.opencypher.spark.impl.physical.RuntimeContext
import org.opencypher.spark.impl.syntax.header.{addContents, _}
import org.opencypher.spark.{BaseTestSuite, SparkTestSession}

import scala.language.implicitConversions

class SparkSQLExprMapperTest extends BaseTestSuite with SparkTestSession.Fixture {

  test("can map subtract") {
    val expr = Subtract(Var("a")(), Var("b")())()

    convert(expr, _header.update(addContent(ProjectedField('foo, expr)))) should equal(Some(
      df.col("a") - df.col("b")
    ))
  }

  private def convert(expr: Expr, header: RecordHeader = _header): Option[Column] = {
    asSparkSQLExpr(header, expr, df)(RuntimeContext.empty)
  }

  val _header: RecordHeader = RecordHeader.empty.update(addContents(Seq(OpaqueField('a), OpaqueField('b))))
  val df: DataFrame = session.createDataFrame(Collections.emptyList[Row](),
    StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType))))

  implicit def extractRecordHeaderFromResult[T](tuple: (RecordHeader, T)): RecordHeader = tuple._1
}
