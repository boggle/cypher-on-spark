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
package org.opencypher.spark.api.classes

import org.opencypher.spark.api.graph.{CypherGraph, CypherResult, GraphSpace}
import org.opencypher.spark.api.record.CypherRecords
import org.opencypher.spark.api.value.CypherValue


trait Cypher {

  self =>

  type Graph <: CypherGraph { type Space = self.Space; type Graph = self.Graph; type Records = self.Records }
  type Space <: GraphSpace { type Space = self.Space; type Graph = self.Graph; type Records = self.Records }
  type Records <: CypherRecords { type Records = self.Records; type Data = self.Data }
  type Result <: CypherResult { type Result = self.Result; type Graph = self.Graph; type Records = self.Records }
  type Data

  final def cypher(graph: Graph, query: String): Result = cypher(graph, query, Map.empty)

  def cypher(graph: Graph, query: String, parameters: Map[String, CypherValue]): Result
}


