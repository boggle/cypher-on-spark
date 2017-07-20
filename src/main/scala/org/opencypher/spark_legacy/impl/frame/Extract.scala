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
package org.opencypher.spark_legacy.impl.frame

import java.lang.Long
import java.{lang => Java}

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.types._
import org.opencypher.spark_legacy.impl._
import org.opencypher.spark.api.value.{CypherMap, CypherNode, CypherRelationship, CypherValue}

import scala.language.postfixOps

object Extract extends FrameCompanion {

  def relationshipStartId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                         (implicit context: PlanningContext): ProjectFrame = {
    val relField = obtain(input.signature.field)(relationship)
    requireMateriallyIsSubTypeOf(relField.cypherType, CTRelationship)
    val (outputField, sig) = input.signature.addField(output -> CTInteger.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipStartId, outputField)(sig)
  }

  def relationshipEndId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                       (implicit context: PlanningContext): ProjectFrame = {
    val relField = obtain(input.signature.field)(relationship)
    requireMateriallyIsSubTypeOf(relField.cypherType, CTRelationship)
    val (outputField, sig) = input.signature.addField(output -> CTInteger.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipEndId, outputField)(sig)
  }

  def relationshipId(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                    (implicit context: PlanningContext): ProjectFrame = {
    val relField = obtain(input.signature.field)(relationship)
    requireMateriallyIsSubTypeOf(relField.cypherType, CTRelationship)
    val (outputField, signature) = input.signature.addField(output -> CTInteger.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipId, outputField)(signature)
  }

  def relationshipType(input: StdCypherFrame[Product])(relationship: Symbol)(output: Symbol)
                      (implicit context: PlanningContext): ProjectFrame = {
    val relField = obtain(input.signature.field)(relationship)
    requireMateriallyIsSubTypeOf(relField.cypherType, CTRelationship)
    val (outputField, signature) = input.signature.addField(output -> CTString.asNullableAs(relField.cypherType))
    new Extract[CypherRelationship](input)(relField, relationshipType, outputField)(signature)
  }

  def nodeId(input: StdCypherFrame[Product])(node: Symbol)(output: Symbol)
            (implicit context: PlanningContext): ProjectFrame = {
    val nodeField = obtain(input.signature.field)(node)
    requireMateriallyIsSubTypeOf(nodeField.cypherType, CTNode)
    val (outputField, signature) = input.signature.addField(output -> CTInteger.asNullableAs(nodeField.cypherType))
    new Extract[CypherNode](input)(nodeField, nodeId, outputField)(signature)
  }

  def property(input: StdCypherFrame[Product])(map: Symbol,propertyKey: Symbol)(output: Symbol)
              (implicit context: PlanningContext): ProjectFrame = {

    val mapField = obtain(input.signature.field)(map)
    requireMateriallyIsSubTypeOf(mapField.cypherType, CTMap)
    val (outputField, signature) = input.signature.addField(output -> CTAny.nullable)
    new Extract[CypherMap](input)(mapField, propertyValue(propertyKey), outputField)(signature)
  }

  private final case class Extract[T](input: StdCypherFrame[Product])(entityField: StdField,
                                                                      primitive: Int => ExtractPrimitive[T],
                                                                      outputField: StdField)
                                     (sig: StdFrameSignature) extends ProjectFrame(outputField, sig) {

    val index = obtain(sig.fieldSlot)(entityField).ordinal

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run
      val out = in.map(primitive(index))(context.productEncoder(slots))
      out
    }
  }

  sealed trait ExtractPrimitive[T] extends (Product => Product) {
    def index: Int
    def extract(value: T): Option[AnyRef]

    import org.opencypher.spark_legacy.impl.util._

    def apply(product: Product): Product = {
      val entity = product.getAs[T](index)
      val value = extract(entity).orNull
      val result = product :+ value
      result
    }
  }

  private final case class relationshipStartId(override val index: Int)
    extends ExtractPrimitive[CypherRelationship] {
      override def extract(relationship: CypherRelationship): Option[Long] =
        CypherRelationship.startId(relationship).map(_.v)
  }

  private final case class relationshipEndId(override val index: Int)
    extends ExtractPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship): Option[Long] =
      CypherRelationship.endId(relationship).map(_.v)
  }

  private final case class nodeId(override val index: Int)
    extends ExtractPrimitive[CypherNode] {
    override def extract(node: CypherNode): Option[Long] =
      CypherNode.id(node).map(_.v)
  }

  private final case class relationshipId(override val index: Int)
    extends ExtractPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship): Option[Long] =
      CypherRelationship.id(relationship).map(_.v)
  }

  private final case class relationshipType(override val index: Int)
    extends ExtractPrimitive[CypherRelationship] {
    override def extract(relationship: CypherRelationship): Option[String] =
      CypherRelationship.relationshipType(relationship)
  }

  private final case class propertyValue(propertyKey: Symbol)(override val index: Int)
    extends ExtractPrimitive[CypherMap] {
    override def extract(v: CypherMap): Option[CypherValue] =
      CypherMap.properties(v).flatMap(_.get(propertyKey.name))
  }
}
