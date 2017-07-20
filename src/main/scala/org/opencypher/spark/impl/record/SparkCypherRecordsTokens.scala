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
package org.opencypher.spark.impl.record

import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.spark.SparkCypherTokens

final case class SparkCypherRecordsTokens(registry: TokenRegistry) extends SparkCypherTokens {

  override type Tokens = SparkCypherRecordsTokens

  override def labels: Set[String] = registry.labels.elts.map(_.name).toSet
  override def relTypes: Set[String] = registry.relTypes.elts.map(_.name).toSet

  override def labelName(id: Int): String = registry.label(LabelRef(id)).name
  override def labelId(name: String): Int = registry.labelRefByName(name).id

  override def relTypeName(id: Int): String = registry.relType(RelTypeRef(id)).name
  override def relTypeId(name: String): Int = registry.relTypeRefByName(name).id

  override def withLabel(name: String) = copy(registry = registry.withLabel(Label(name)))
  override def withRelType(name: String) = copy(registry = registry.withRelType(RelType(name)))
}
