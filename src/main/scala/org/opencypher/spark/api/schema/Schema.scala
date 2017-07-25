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
package org.opencypher.spark.api.schema

import org.opencypher.spark.api.types._
import org.opencypher.spark.api.util.{Verifiable, Verified}

import scala.language.implicitConversions

object Schema {
  val empty: Schema = Schema(
    labels = Set.empty,
    relationshipTypes = Set.empty,
    nodeKeyMap = PropertyKeyMap.empty,
    relKeyMap = PropertyKeyMap.empty,
    impliedLabels = ImpliedLabels(Map.empty),
    labelCombinations = LabelCombinations(Set.empty)
  )
}

object PropertyKeyMap {
  val empty = PropertyKeyMap(Map.empty)()

  /**
    * Updates all contained cypher types to their corresponding nullable type.
    *
    * @param map property key map
    * @return updated property key map
    */
  def asNullable(map: PropertyKeyMap): PropertyKeyMap = PropertyKeyMap(map.m.map {
    pair => pair._1 -> pair._2.map(p2 => p2._1 -> p2._2.nullable)
  })()
}

final case class PropertyKeyMap(m: Map[String, Map[String, CypherType]])(val conflicts: Set[String] = Set.empty) {
  def keysFor(classifier: String): Map[String, CypherType] = m.getOrElse(classifier, Map.empty)
  def withKeys(classifier: String, keys: Seq[(String, CypherType)]): PropertyKeyMap = {
    val oldKeys = m.getOrElse(classifier, Map.empty)
    val newKeys = keys.toMap
    val newConflicts = oldKeys.collect {
      case (k, t) =>
        newKeys.get(k) match {
          case Some(otherT) if t != otherT =>
            Some(s"Conflicting schema for '$classifier'! Key '$k' has type $t but also has type ${newKeys(k)}")
          case _ =>
            None
        }
    }.flatten.toSet
    copy(m.updated(classifier, oldKeys ++ newKeys))(conflicts = conflicts ++ newConflicts)
  }

  def keys = m.values.flatMap(_.keySet).toSet

  def ++(other: PropertyKeyMap) = copy(m ++ other.m)(conflicts ++ other.conflicts)
}

case class ImpliedLabels(m: Map[String, Set[String]]) {

  def transitiveImplicationsFor(known: Set[String]): Set[String] = {
    val next = known.flatMap(implicationsFor)
    if (next == known) known else transitiveImplicationsFor(next)
  }

  def withImplication(source: String, target: String): ImpliedLabels = {
    val implied = implicationsFor(source)
    if (implied(target)) this else copy(m = m.updated(source, implied + target))
  }

  def toPairs: Set[(String, String)] = {
    m.toArray
      .flatMap(pair => pair._2.map(elem => (pair._1, elem)))
      .toSet
  }

  private def implicationsFor(source: String) = m.getOrElse(source, Set.empty) + source
}

case class LabelCombinations(combos: Set[Set[String]]) {

  def combinationsFor(label: String): Set[String] = combos.find(_(label)).getOrElse(Set.empty)

  def withCombinations(as: String*): LabelCombinations = {
    val (lhs, rhs) = combos.partition(labels => as.exists(labels(_)))
    copy(combos = rhs + (lhs.flatten ++ as))
  }

  def ++(other: LabelCombinations) = copy(combos ++ other.combos)
}

final case class Schema(
  /**
   * All labels present in this graph
   */
  labels: Set[String],
  /**
   * All relationship types present in this graph
   */
  relationshipTypes: Set[String],
  /**
    * Property keys associated with a node label
    */
  nodeKeyMap: PropertyKeyMap,
  /**
    * Property keys associated with a relationship type
    */
  relKeyMap: PropertyKeyMap,
  /**
    * Implied labels for each existing label
    */
  impliedLabels: ImpliedLabels,
  /**
    * Groups of labels where each group contains possible label combinations.
    */
  labelCombinations: LabelCombinations) extends Verifiable {

  self: Schema =>

  override type Self = Schema
  override type VerifiedSelf = VerifiedSchema

  /**
   * Given a set of labels that a node definitely has, returns all labels the node _must_ have.
   */
  def impliedLabels(knownLabels: Set[String]): Set[String] =
    impliedLabels.transitiveImplicationsFor(knownLabels.intersect(labels))

  /**
   * Given a set of labels that a node definitely has, returns all the labels that the node could possibly have.
   */
  def labelCombination(knownLabels: Set[String]): Set[String] =
    knownLabels.flatMap(labelCombinations.combinationsFor)

  /**
   * Given a label that a node definitely has, returns its property schema.
    *
    * TODO: consider implied labels here?
   */
  def nodeKeys(label: String): Map[String, CypherType] = nodeKeyMap.keysFor(label)

  def keys = nodeKeyMap.keys ++ relKeyMap.keys

  lazy val conflictSet = nodeKeyMap.conflicts ++ relKeyMap.conflicts

  /**
   * Returns the property schema for a given relationship type
   */
  def relationshipKeys(typ: String): Map[String, CypherType] = relKeyMap.keysFor(typ)

  def withImpliedLabel(pair: (String, String)): Schema = withImpliedLabel(pair._1, pair._2)

  def withImpliedLabel(existingLabel: String, impliedLabel: String): Schema =
    copy(labels = labels + existingLabel + impliedLabel,
      impliedLabels = impliedLabels.withImplication(existingLabel, impliedLabel))

  def withLabelCombination(pair: (String, String)): Schema = withLabelCombination(pair._1, pair._2)

  def withLabelCombination(as: String*): Schema =
    copy(labels = labels ++ as, labelCombinations = labelCombinations.withCombinations(as: _*))

  def withNodePropertyKeys(label: String)(keys: (String, CypherType)*): Schema =
    copy(labels = labels + label, nodeKeyMap = nodeKeyMap.withKeys(label, keys))

  def withRelationshipPropertyKeys(typ: String)(keys: (String, CypherType)*): Schema =
    copy(relationshipTypes = relationshipTypes + typ, relKeyMap = relKeyMap.withKeys(typ, keys))

  def ++(other: Schema) = {
    val newLabels = labels ++ other.labels
    val newRelTypes = relationshipTypes ++ other.relationshipTypes
    val newNodeKeyMap = nodeKeyMap ++ other.nodeKeyMap
    val newRelKeyMap = relKeyMap ++ other.relKeyMap
    val newImpliedLabels = inferImpliedLabels(other)

    // new optional labels are previous optional labels and all revoked implied labels
    val combinedOptionalLabels = this.labelCombinations ++ other.labelCombinations
    val newLabelCombinationPairs = this.impliedLabels.toPairs ++ other.impliedLabels.toPairs -- newImpliedLabels.toPairs
    val newLabelCombinations = newLabelCombinationPairs.foldLeft(combinedOptionalLabels)((o,p) => o.withCombinations(p._1, p._2))

    copy(newLabels,
      newRelTypes,
      newNodeKeyMap,
      newRelKeyMap,
      newImpliedLabels,
      newLabelCombinations)
  }

  /**
    * Computes the resulting implied labels from the current and the given schema.
    *
    * Example:
    *
    * this.labels = {A, B, C}
    * this.impliedLabels:
    * A -> B
    * B -> C
    *
    * other.labels = {B, C, D}
    * other.impliedLabels
    * B -> C
    * C -> D
    *
    * (1) compute intersection between this.impliedLabels and other.impliedLabels
    * (2) compute implied labels exclusive for left and right side
    * (3) from exclusive labels remove those pairs where the first item is not contained in the other one's label set
    * (4) union intersecting pairs with filtered pairs from both sides
    *
    * @param other other schema
    * @return implied labels inferred from the given implied labels
    */
  private def inferImpliedLabels(other: Schema) = {
    val leftImpliedPairs = this.impliedLabels.toPairs
    val rightImpliedPairs = other.impliedLabels.toPairs

    val intersectPairs = leftImpliedPairs intersect rightImpliedPairs
    val exclusivePairsLeft = (leftImpliedPairs -- intersectPairs)
      .filterNot(pair => other.labels.contains(pair._1))
    val exclusivePairsRight = (rightImpliedPairs -- intersectPairs)
      .filterNot(pair => this.labels.contains(pair._1))

    ImpliedLabels((exclusivePairsLeft ++ exclusivePairsRight ++ intersectPairs)
      .groupBy(_._1)
      .map(pair => pair._1 -> pair._2.map(_._2)))
  }

  override def verify: VerifiedSchema = {
    // TODO:
    //
    // We envision this to change in two ways
    // (1) Only enforce correct types for a property key between implied labels
    // (2) Use union types (and generally support them) for combined labels
    //

    if (conflictSet.nonEmpty) {
      conflictSet.foreach(println)
      throw new IllegalStateException(s"Schema invalid: $self")
    }

    val coOccurringLabels =
      for (
        label <- labels;
        other <- impliedLabels(Set(label)) ++ labelCombination(Set(label))
      ) yield label -> other

    for ((label, other) <- coOccurringLabels) {
      val xKeys = nodeKeys(label)
      val yKeys = nodeKeys(other)
      if (xKeys.keySet.intersect(yKeys.keySet).exists(key => xKeys(key) != yKeys(key)))
        throw new IllegalArgumentException(s"Failed to verify schema for labels :$label and :$other")
    }

    new VerifiedSchema {
      override def schema = self
    }
  }

  override def toString = {
    val builder = new StringBuilder

    builder.append("Node labels:\n")
    labels.foreach { label =>
      builder.append(s":$label\n")
      nodeKeys(label).foreach {
        case (key, typ) => builder.append(s"\t$key: $typ\n")
      }
    }

    builder.append("Implied labels:\n")
    impliedLabels.m.foreach { pair =>
      builder.append(s":${pair._1} -> ${pair._2}\n")
    }

    builder.append("Label combinations:\n")
    this.labelCombinations.combos.foreach { set =>
      builder.append(s"$set\n")
    }

    builder.append("Rel types:\n")
    relationshipTypes.foreach { relType =>
      builder.append(s":$relType\n")
      relationshipKeys(relType).foreach {
        case (key, typ) => builder.append(s"\t$key: $typ\n")
      }
    }

    builder.toString
  }
}

sealed trait VerifiedSchema extends Verified[Schema] {
  final override def v = schema
  def schema: Schema
}
