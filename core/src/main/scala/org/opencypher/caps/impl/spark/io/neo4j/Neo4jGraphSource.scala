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
package org.opencypher.caps.impl.spark.io.neo4j

import java.net.URI

import org.opencypher.caps.api.io.{CreateOrFail, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.impl.spark.io.CAPSGraphSourceImpl

case class Neo4jGraphSource(config: EncryptedNeo4jConfig,
                            nodeQuery: String,
                            relQuery: String)(implicit capsSession: CAPSSession)
  extends CAPSGraphSourceImpl {

  import org.opencypher.caps.impl.spark.io.neo4j.Neo4jGraphSourceFactory.supportedSchemes

  override def sourceForGraphAt(uri: URI): Boolean =
    supportedSchemes.contains(uri.getScheme) && uri.getHost == config.uri.getHost && uri.getPort == config.uri.getPort

  override def graph: CAPSGraph =
    Neo4jGraphLoader.fromNeo4j(config, nodeQuery, relQuery)

  override def schema: Option[Schema] = None

  override def canonicalURI: URI = {
    val uri = config.uri
    val host = uri.getHost
    val port = if (uri.getPort == -1) "" else s":${uri.getPort}"
    val canonicalURIString = s"${uri.getScheme}://$host$port"
    URI.create(canonicalURIString)
  }

  override def create: CAPSGraph =
    persist(CreateOrFail, CAPSGraph.empty)

  override def persist(mode: PersistMode, graph: CAPSGraph): CAPSGraph =
    ???

  override def delete(): Unit =
    ???
}




