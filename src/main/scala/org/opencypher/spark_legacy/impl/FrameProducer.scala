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
package org.opencypher.spark_legacy.impl

import org.apache.spark.sql.Row
import org.opencypher.spark_legacy.impl.frame._
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.api.value.{CypherNode, CypherRelationship, CypherValue}

class FrameProducer(implicit val planningContext: PlanningContext) {

  def allNodes(sym: Symbol) = AllNodes(sym)
  def labelScan(sym: Symbol)(labels: IndexedSeq[String]) =
    if (labels.isEmpty) allNodes(sym)
    else allNodes(sym).labelFilter(labels:_*)
  def optionalAllNodes(sym: Symbol) = OptionalAllNodes(sym)
  def allRelationships(sym: Symbol) = AllRelationships(sym)
  def typeScan(sym: Symbol)(types: IndexedSeq[String]) =
    if (types.isEmpty) allRelationships(sym)
    else allRelationships(sym).typeFilter(types:_*)
  def optionalAllRelationships(sym: Symbol) = OptionalAllRelationships(sym)

  abstract class AbstractRichFrame[T](input: StdCypherFrame[T]) {
    def upcast(sym: Symbol)(widen: CypherType => CypherType): StdCypherFrame[T] =
      Upcast(input)(sym)(widen)
  }

  abstract class AbstractRichValueFrame[T <: CypherValue](input: StdCypherFrame[T])
    extends AbstractRichFrame[T](input) {
    def asProduct = ValueAsProduct(input)
    def asRow = ValueAsRow(input)
  }

  implicit final class RichValueFrame[T <: CypherValue](input: StdCypherFrame[T])
    extends AbstractRichValueFrame[T](input)

  implicit final class RichNodeFrame(input: StdCypherFrame[CypherNode])
    extends AbstractRichValueFrame[CypherNode](input) {
    def labelFilter(labels: String*) = LabelFilterNode(input)(labels)
  }

  implicit final class RichRelationshipFrame(input: StdCypherFrame[CypherRelationship])
    extends AbstractRichValueFrame[CypherRelationship](input) {
    def typeFilter(types: String*) = TypeFilterRelationship(input)(types.toSet)
  }

  implicit final class RichProductFrame(input: StdCypherFrame[Product])
    extends AbstractRichFrame[Product](input) {
    def asRow = ProductAsRow(input)

    def unionAll(other: StdCypherFrame[Product]) =
      UnionAll(input, other)

    def propertyValue(node: Symbol, propertyKey: Symbol)(outputName: Symbol) =
      Extract.property(input)(node, propertyKey)(outputName)

    def aliasField(alias: (Symbol, Symbol)) = {
      val (oldName, newName) = alias
      AliasField(input)(oldName)(newName)
    }

    def selectFields(fields: Symbol*) =
      SelectProductFields(input)(fields: _*)

    def relationshipStartId(entity: Symbol)(output: Symbol) =
      Extract.relationshipStartId(input)(entity)(output)

    def relationshipEndId(entity: Symbol)(output: Symbol) =
      Extract.relationshipEndId(input)(entity)(output)

    def nodeId(entity: Symbol)(output: Symbol) =
      Extract.nodeId(input)(entity)(output)

    def relationshipId(entity: Symbol)(output: Symbol) =
      Extract.relationshipId(input)(entity)(output)

    def relationshipType(entity: Symbol)(output: Symbol) =
      Extract.relationshipType(input)(entity)(output)

    def orderBy(item: SortItem) = OrderBy(input)(item)

    def groupBy(key: Symbol*)(agg: AggregationFunction) =
      GroupBy(input)(key:_*)(agg)

    def unwind(list: Symbol, item: Symbol) =
      Unwind(input)(list, item)

    def varExpand(node: Symbol, lowerBound: Int, upperBound: Int)(relationships: Symbol) =
      VarExpand(input)(node, lowerBound, upperBound)(relationships)

    def dropField(field: Symbol) =
      DropField(input)(field)

    def labelFilter(field: Symbol, labels: Set[String]) =
      FilterProduct.labelFilter(input)(field, labels.toSeq)
  }

  implicit final class RichRowFrame(input: StdCypherFrame[Row])
    extends AbstractRichFrame[Row](input) {
    def asProduct = RowAsProduct(input)

    def join(other: StdCypherFrame[Row]) = new JoinBuilder[Row] {
      def on(lhsKey: Symbol)(rhsKey: Symbol): StdCypherFrame[Row] =
        Join(input, other)(lhsKey, rhsKey)(optional = false)
    }

    def optionalJoin(other: StdCypherFrame[Row]) = new JoinBuilder[Row] {
      def on(lhsKey: Symbol)(rhsKey: Symbol): StdCypherFrame[Row] =
        Join(input, other)(lhsKey, rhsKey)(optional = true)
    }
  }

  sealed trait JoinBuilder[T] {
    def on(lhsKey: Symbol)(rhsKey: Symbol): StdCypherFrame[T]
  }
}
