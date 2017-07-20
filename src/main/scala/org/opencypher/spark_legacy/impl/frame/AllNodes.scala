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

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark_legacy.impl._
import org.opencypher.spark.api.value.CypherNode

object AllNodes extends FrameCompanion {

  def apply(fieldSym: Symbol)(implicit context: PlanningContext): StdCypherFrame[CypherNode] = {
    val (_, sig) = StdFrameSignature.empty.addField(fieldSym -> CTNode)
    CypherNodes(
      input = context.nodes,
      sig = sig
    )
  }

  private final case class CypherNodes(input: Dataset[CypherNode], sig: StdFrameSignature)
    extends NodeFrame(sig) {

    override def execute(implicit context: RuntimeContext): Dataset[CypherNode] = input
  }
}
