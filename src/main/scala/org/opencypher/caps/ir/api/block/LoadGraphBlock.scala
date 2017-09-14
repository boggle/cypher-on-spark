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
package org.opencypher.caps.ir.api.block

import java.net.URI

import org.opencypher.caps.ir.api.IRField
import org.opencypher.caps.ir.api.pattern.AllGiven

case class LoadGraphBlock[E](
  after: Set[BlockRef],
  binds: GraphDescriptor[E],
  source: URI
) extends BasicBlock[GraphDescriptor[E], E](BlockType("load-graph")) {
  override def where: AllGiven[E] = AllGiven[E]()
}

sealed trait GraphDescriptor[E] extends Binds[E] {
  override def fields: Set[IRField] = Set.empty
}

final case class AmbientGraph[E]() extends GraphDescriptor[E]
