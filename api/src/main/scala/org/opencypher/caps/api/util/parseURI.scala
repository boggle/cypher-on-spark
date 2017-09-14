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
package org.opencypher.caps.api.util

import java.net.URI

// TODO: Test
case object parseURI extends (String => URI) {

  def apply(pathOrUri: String): URI = {
    val trimmed = pathOrUri.trim
    if (trimmed.startsWith("/"))
      URI.create(s"session:$trimmed")
    else
      URI.create(trimmed)
  }
}
