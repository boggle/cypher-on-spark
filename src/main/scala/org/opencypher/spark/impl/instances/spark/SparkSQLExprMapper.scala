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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, StringType}
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.Label
import org.opencypher.spark.api.record.RecordHeader
import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark.api.value.CypherValue
import org.opencypher.spark.impl.convert.{toJavaType, toSparkType}
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.physical.RuntimeContext
import org.opencypher.spark.impl.spark.SparkColumnName

import scala.collection.mutable

object SparkSQLExprMapper {

  private def verifyExpression(header: RecordHeader, expr: Expr) = {
    val slots = header.slotsFor(expr)

    if (slots.isEmpty) {
      Raise.slotNotFound(expr.toString)
    } else if (slots.size > 1 && !expr.isInstanceOf[Var]) {
      Raise.notYetImplemented("support for multi-column expressions")
    }
  }

  private def getColumn(expr: Expr, header: RecordHeader, dataFrame: DataFrame)
                       (implicit context: RuntimeContext): Column = {
    expr match {
      case c: Const =>
        udf(const(context.parameters(context.constants.constantRef(c.constant))), toSparkType(c.cypherType))()
      case _ =>
        verifyExpression(header, expr)
        val slot = header.slotsFor(expr).head

        dataFrame.col(context.columnName(slot))
    }
  }
  object asSparkSQLExpr {

    /**
      * Attempts to create a Spark SQL expression from the SparkCypher expression.
      *
      * @param header  the header of the SparkCypherRecords in which the expression should be evaluated.
      * @param expr    the expression to be evaluated.
      * @param df      the dataframe containing the data over which the expression should be evaluated.
      * @param context context with helper functions, such as column names.
      * @return Some Spark SQL expression if the input was mappable, otherwise None.
      */
    def apply(header: RecordHeader, expr: Expr, df: DataFrame)
             (implicit context: RuntimeContext): Option[Column] = expr match {

      case _: Var =>
        val col = getColumn(expr, header, df)
        Some(col)

      case _: Const => Some(getColumn(expr, header, df))

      // predicates
      case Equals(v1: Var, v2: Var) =>
        val lCol = getColumn(v1, header, df)
        val rCol = getColumn(v2, header, df)
        Some(lCol === rCol)

      case Not(e) =>
        apply(header, e, df) match {
          case Some(res) => Some(!res)
          case _ => Raise.notYetImplemented(s"Support for expression $e")
        }

      case Ands(exprs) =>
        val cols = exprs.map(asSparkSQLExpr(header, _, df))
        if (cols.contains(None)) None
        else {
          cols.reduce[Option[Column]] {
            // TODO: Does this work with Cypher's ternary logic?
            case (Some(l: Column), Some(r: Column)) => Some(l && r)
            case _ => Raise.impossible()
          }
        }

      case Ors(exprs) =>
        val cols = exprs.map(asSparkSQLExpr(header, _, df))
        if (cols.contains(None)) None
        else {
          cols.reduce[Option[Column]] {
            case (Some(l: Column), Some(r: Column)) => Some(l || r)
            case _ => Raise.impossible()
          }
        }

      case HasType(rel, relType) =>
        val relTypeId = context.tokens.relTypeRef(relType).id
        val col = getColumn(TypeId(rel)(), header, df)
        Some(col === relTypeId)

      case h: HasLabel =>
        Some(getColumn(h, header, df)) // it's a boolean column

      case inEq: LessThan => Some(inequality(lt, header, inEq, df))
      case inEq: LessThanOrEqual => Some(inequality(lteq, header, inEq, df))
      case inEq: GreaterThanOrEqual => Some(inequality(gteq, header, inEq, df))
      case inEq: GreaterThan => Some(inequality(gt, header, inEq, df))

      // Arithmetics
      case add: Add =>
        verifyExpression(header, expr)

        val lhsColumn = getColumn(add.lhs, header, df)
        val rhsColumn = getColumn(add.rhs, header, df)
        Some(lhsColumn + rhsColumn)

      case sub: Subtract =>
        verifyExpression(header, expr)

        val lhsColumn = getColumn(sub.lhs, header, df)
        val rhsColumn = getColumn(sub.rhs, header, df)
        Some(lhsColumn - rhsColumn)

      case mul: Multiply =>
        verifyExpression(header, expr)

        val lhsColumn = getColumn(mul.lhs, header, df)
        val rhsColumn = getColumn(mul.rhs, header, df)
        Some(lhsColumn * rhsColumn)

      case div: Divide =>
        verifyExpression(header, expr)

        val lhsColumn = getColumn(div.lhs, header, df)
        val rhsColumn = getColumn(div.rhs, header, df)
        Some((lhsColumn / rhsColumn).cast(toSparkType(div.cypherType)))

      // Functions
      case id: Id =>
        verifyExpression(header, expr)

        val column = getColumn(id.expr, header, df)
        Some(column)

      case labels: Labels =>
        verifyExpression(header, expr)

        val node = Var(SparkColumnName.of(header.slotsFor(labels.expr).head))(CTNode)
        val labelExprs = header.labels(node)
        val labelColumns = labelExprs.map(getColumn(_, header, df))
        val labelNames = labelExprs.map(_.label)
        val labelsUDF = udf(getNodeLabels(labelNames), ArrayType(StringType, containsNull = false))
        Some(labelsUDF(functions.array(labelColumns: _*)))

      case _ =>
        None
    }

    private def inequality(f: (Any, Any) => Any, header: RecordHeader, expr: BinaryExpr, df: DataFrame)
                          (implicit context: RuntimeContext): Column = {
      verifyExpression(header, expr)

      val lhsColumn = getColumn(expr.lhs, header, df)
      val rhsColumn = getColumn(expr.rhs, header, df)

      udf(f, BooleanType)(lhsColumn, rhsColumn)
    }
  }

  // TODO: Move to UDF package
  import org.opencypher.spark.api.value.CypherValueUtils._

  private def const(v: CypherValue): () => Any = () => toJavaType(v)

  // TODO: Try to share code with cypherFilter()
  private def lt(lhs: Any, rhs: Any): Any = (CypherValue(lhs) < CypherValue(rhs)).orNull
  private def lteq(lhs: Any, rhs: Any): Any = (CypherValue(lhs) <= CypherValue(rhs)).orNull
  private def gteq(lhs: Any, rhs: Any): Any = (CypherValue(lhs) >= CypherValue(rhs)).orNull
  private def gt(lhs: Any, rhs: Any): Any = (CypherValue(lhs) > CypherValue(rhs)).orNull
  private def getNodeLabels(labelNames: Seq[Label])= (labelSwitches: Any) =>
    labelSwitches match {
      case a:mutable.WrappedArray[Boolean] =>
        a.zip(labelNames).collect({
          case (true, label) => label.name
        }).toArray
    }

}
