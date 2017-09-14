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
package org.opencypher.caps.api.record

import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.types._

final case class RecordSlot(index: Int, content: SlotContent)

object RecordSlot {
  def from(pair: (Int, SlotContent)) = RecordSlot(pair._1, pair._2)
}

sealed trait SlotContent {

  def key: Expr

  def alias: Option[Var]
  def owner: Option[Var]

  final def cypherType: CypherType = key.cypherType
  def support: Traversable[Expr]
}

sealed trait ProjectedSlotContent extends SlotContent {

  def expr: Expr

  override def owner = expr match {
    case Property(v: Var, _) => Some(v)
    case HasLabel(v: Var, _) => Some(v)
    case HasType(v: Var, _) => Some(v)
    case StartNode(v: Var) => Some(v)
    case EndNode(v: Var) => Some(v)
    case TypeId(v: Var) => Some(v)
    case _ => None
  }
}

sealed trait FieldSlotContent extends SlotContent {
  override def alias = Some(field)
  override def key: Var = field
  def field: Var
}

final case class ProjectedExpr(expr: Expr) extends ProjectedSlotContent {
  override def key = expr
  override def alias = None

  override def support = Seq(expr)
}

final case class OpaqueField(field: Var) extends FieldSlotContent {
  override def owner = Some(field)

  override def support = Seq(field)
}

final case class ProjectedField(field: Var, expr: Expr)
  extends ProjectedSlotContent with FieldSlotContent {

  override def support = Seq(field, expr)
}
