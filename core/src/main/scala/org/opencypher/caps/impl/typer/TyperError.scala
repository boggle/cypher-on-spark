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
package org.opencypher.caps.impl.typer

import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression
import org.opencypher.caps.api.types._
import cats.syntax.show._

sealed trait TyperError

case class UnsupportedExpr(expr: Expression) extends TyperError {
  override def toString = s"Don't know how to type ${expr.show}"
}

case class UnTypedExpr(it: Expression) extends TyperError {
  override def toString = s"Expected a type for ${it.show} but found none"
}

case class NoSuitableSignatureForExpr(it: Expression, argTypes: Seq[CypherType]) extends TyperError {
  override def toString = s"No signature for ${it.show} matched input types ${argTypes.mkString("{ ", ", ", " }")}"
}

case class AlreadyTypedExpr(it: Expression, oldTyp: CypherType, newTyp: CypherType) extends TyperError {
  override def toString = s"Tried to type ${it.show} with $newTyp but it was already typed as $oldTyp"
}

case class InvalidContainerAccess(it: Expression) extends TyperError {
  override def toString = s"Invalid indexing into a container detected when typing ${it.show}"
}

object InvalidType {
  def apply(it: Expression, expected: CypherType, actual: CypherType): InvalidType =
    InvalidType(it, Seq(expected), actual)
}

case class InvalidType(it: Expression, expected: Seq[CypherType], actual: CypherType) extends TyperError {
  override def toString = s"Expected ${it.show} to have $expectedString, but it was of type $actual"

  private def expectedString =
    if (expected.size == 1) s"type ${expected.head}"
    else s"one of the types in ${expected.mkString("{ ", ",", " }")}"
}

case object TypeTrackerScopeError extends TyperError {
  override def toString = "Tried to pop scope of type tracker, but it was at top level already"
}

case class InvalidArgument(expr: Expression, argument: Expression) extends TyperError {
  override def toString = s"$argument is not a valid argument for $expr"
}

case class WrongNumberOfArguments(expr: Expression, expected: Int, actual: Int) extends TyperError {
  override def toString = s"Expected $expected argument(s) for $expr, but got $actual"
}
