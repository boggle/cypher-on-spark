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
package org.opencypher.caps.api.io

import java.net.URI

import org.opencypher.caps.api.graph.{CypherGraph, CypherSession}

trait GraphSourceFactory {

  self =>

  type Session <: CypherSession { type Session = self.Session; type Graph = self.Graph }
  type Graph <: CypherGraph { type Session = self.Session; type Graph = self.Graph }
  type Source <: GraphSource { type Session = self.Session; type Graph = self.Graph }

  /**
    * A simple name for the graph source factory
    *
    * @return a name that describes this factory
    */
  def name: String

  /**
    * The protocol for which this factory produces graph sources.
    *
    * @return that protocol
    */
  def schemes: Set[String]

  /**
    * Creates a new graph source at the argument uri.
    *
    * @param uri at which a new graph source is to be created
    * @return create a new graph source for the given uri
    * @throws RuntimeException if uri is not supported by this graph source factory
    */
  def sourceFor(uri: URI)(implicit session: Session): Source
}
