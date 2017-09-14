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
package org.opencypher.caps.impl.flat

import cats.Monoid
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.logical.{GraphSource, NamedLogicalGraph}
import org.opencypher.caps.api.record.{Added, FailedToAdd, Found, Replaced}
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.api.syntax.expr._
import org.opencypher.caps.api.syntax.header._
import org.opencypher.caps.ir.api.block.SortItem
import org.opencypher.caps.ir.api.pattern.{EveryNode, EveryRelationship}

class FlatOperatorProducer(implicit context: FlatPlannerContext) {

  private val tokens = context.tokens
  private val schema = context.schema

  private implicit val typeVectorMonoid = new Monoid[Vector[CypherType]] {
    override def empty: Vector[CypherType] = Vector.empty
    override def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType] = x ++ y
  }

  def select(fields: IndexedSeq[Var], in: FlatOperator): Select = {
    val fieldContents = fields.map { field => in.header.slotsFor(field).head.content }
    val exprContents = in.header.contents.collect {
      case content@ProjectedExpr(expr) if (expr.dependencies -- fields).isEmpty => content
    }
    val finalContents = fieldContents ++ exprContents

    val (nextHeader, _) = RecordHeader.empty.update(addContents(finalContents))

    Select(fields, in, nextHeader)
  }

  def filter(expr: Expr, in: FlatOperator): Filter = {
    in.header

//    expr match {
//      case HasLabel(n, label) =>
//        in.header.contents.map { c =>
//
//        }
//      case _ => in.header
//    }

    // TODO: Should replace SlotContent expressions with detailed type of entity
    // TODO: Should reduce width of header due to more label information

    Filter(expr, in, in.header)
  }

  def distinct(fields: Set[Var], in: FlatOperator): Distinct = {
    val (newHeader, _) = RecordHeader.empty.update(
      addContents(fields.toSeq.map(OpaqueField))
    )
    Distinct(in, newHeader)
  }

  def nodeScan(node: Var, nodeDef: EveryNode, prev: FlatOperator): NodeScan = {
    val header = if (nodeDef.labels.elements.isEmpty) RecordHeader.nodeFromSchema(node, schema, tokens)
    else RecordHeader.nodeFromSchema(node, schema, tokens, nodeDef.labels.elements.map(_.name))

    new NodeScan(node, nodeDef, prev, header)
  }

  def edgeScan(edge: Var, edgeDef: EveryRelationship, prev: FlatOperator): EdgeScan = {
    val edgeHeader = if (edgeDef.relTypes.elements.isEmpty) RecordHeader.relationshipFromSchema(edge, schema, tokens)
    else RecordHeader.relationshipFromSchema(edge, schema, tokens, edgeDef.relTypes.elements.map(_.name))

    EdgeScan(edge, edgeDef, prev, edgeHeader)
  }

  def varLengthEdgeScan(edgeList: Var, edgeDef: EveryRelationship, prev: FlatOperator): EdgeScan = {
    val edge = FreshVariableNamer(edgeList.name + "extended", CTRelationship)
    edgeScan(edge, edgeDef, prev)
  }

  def aggregate(aggregations: Set[(Var, Aggregator)], group: Set[Var], in: FlatOperator): Aggregate = {
    val (newHeader, _) = RecordHeader.empty.update(
      addContents(group.toSeq.map(OpaqueField) ++ aggregations.map(agg => OpaqueField(agg._1)))
    )

    Aggregate(aggregations, group, in, newHeader)
  }

  // TODO: Specialize per kind of slot content
  def project(it: ProjectedSlotContent, in: FlatOperator): FlatOperator = {
    val (newHeader, result) = in.header.update(addContent(it))

    result match {
      case _: Found[_] => in
      case _: Replaced[_] => Alias(it.expr, it.alias.get, in, newHeader)
      case _: Added[_] => Project(it.expr, in, newHeader)
      case f: FailedToAdd[_] => Raise.slotNotAdded(f.toString)
    }
  }

  // TODO: Specialize per kind of slot content
  // TODO: Remove types parameter and read rel-types from the rel variable
  def expandSource(source: Var, rel: Var, types: EveryRelationship, target: Var,
                   sourceOp: FlatOperator, targetOp: FlatOperator): FlatOperator = {
    val relHeader =
      if (types.relTypes.elements.isEmpty) RecordHeader.relationshipFromSchema(rel, schema, tokens)
      else RecordHeader.relationshipFromSchema(rel, schema, tokens, types.relTypes.elements.map(_.name))

    val expandHeader = sourceOp.header ++ relHeader ++ targetOp.header

    ExpandSource(source, rel, types, target, sourceOp, targetOp, expandHeader, relHeader)
  }

  def expandInto(source: Var, rel: Var, types: EveryRelationship, target: Var, sourceOp: FlatOperator): FlatOperator = {
    val relHeader =
      if (types.relTypes.elements.isEmpty) RecordHeader.relationshipFromSchema(rel, schema, tokens)
      else RecordHeader.relationshipFromSchema(rel, schema, tokens, types.relTypes.elements.map(_.name))

    val expandHeader = sourceOp.header ++ relHeader

    ExpandInto(source, rel, types, target, sourceOp, expandHeader, relHeader)
  }

  def planStart(logicalGraph: NamedLogicalGraph, source: GraphSource, fields: Set[Var]): Start = {
    Start(logicalGraph, source, fields)
  }

  def initVarExpand(source: Var, edgeList: Var, in: FlatOperator): InitVarExpand = {
    val endNodeId = FreshVariableNamer(edgeList.name + "endNode", CTNode)
    val (header, _) = in.header.update(addContents(Seq(OpaqueField(edgeList), OpaqueField(endNodeId))))

    InitVarExpand(source, edgeList, endNodeId, in, header)
  }

  def boundedVarExpand(edge: Var, edgeList: Var, target: Var, lower: Int, upper: Int,
                       sourceOp: InitVarExpand, edgeOp: FlatOperator, targetOp: FlatOperator) : FlatOperator = {

    val (initHeader, _) = sourceOp.in.header.update(addContent(OpaqueField(edgeList)))
    val header = initHeader ++ targetOp.header

    BoundedVarExpand(edge, edgeList, target, lower, upper, sourceOp, edgeOp, targetOp, header)
  }

  def planOptional(lhs: FlatOperator, rhs: FlatOperator): FlatOperator = {
    Optional(lhs, rhs, lhs.header, rhs.header)
  }

  def orderBy(sortItems: Seq[SortItem[Expr]], sourceOp: FlatOperator): FlatOperator = {
    OrderBy(sortItems, sourceOp, sourceOp.header)
  }

  def skip(expr: Expr, sourceOp: FlatOperator): FlatOperator = {
    Skip(expr, sourceOp, sourceOp.header)
  }

  def limit(expr: Expr, sourceOp: FlatOperator): FlatOperator = {
    Limit(expr, sourceOp, sourceOp.header)
  }
}
