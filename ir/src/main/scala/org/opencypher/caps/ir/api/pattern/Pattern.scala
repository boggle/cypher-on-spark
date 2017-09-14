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
package org.opencypher.caps.ir.api.pattern

import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.block.Binds

case object Pattern {
  def empty[E] = Pattern[E](entities = Map.empty, topology = Map.empty)
}

final case class Pattern[E](entities: Map[IRField, EveryEntity], topology: Map[IRField, Connection]) extends Binds[E] {

  lazy val nodes: Map[IRField, EveryNode] = entities.collect { case (k, v: EveryNode) => k -> v }
  lazy val rels: Map[IRField, EveryRelationship] = entities.collect { case (k, v: EveryRelationship) => k -> v }

  override def fields = entities.keySet

  def connectionsFor(node: IRField): Map[IRField, Connection] = {
    topology.filter {
      case (_, c) => c.endpoints.contains(node)
    }
  }

  def withoutConnection(rel: IRField): Pattern[E] = {
    val c = topology(rel)
    copy(entities = entities - c.source - c.target - rel,
         topology = topology - rel)
  }

  def solvedNode(key: IRField): Pattern[E] = {
    copy(entities = entities - key)
  }

  def isEmpty: Boolean = this == Pattern.empty

  def withConnection(key: IRField, connection: Connection): Pattern[E] =
    if (topology.get(key).contains(connection)) this else copy(topology = topology.updated(key, connection))

  def withEntity(key: IRField, value: EveryEntity): Pattern[E] =
    if (entities.get(key).contains(value)) this else copy(entities = entities.updated(key, value))
}

