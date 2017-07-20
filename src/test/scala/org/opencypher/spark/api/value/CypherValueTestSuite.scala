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
package org.opencypher.spark.api.value

import org.opencypher.spark.BaseTestSuite
import org.opencypher.spark.api.types.{False, Maybe, Ternary, True}

import scala.annotation.tailrec

class CypherValueTestSuite extends BaseTestSuite with CypherValue.Conversion {

  @tailrec
  final def isPathLike(l: Seq[Any], nextIsNode: Ternary = Maybe): Boolean = l match {
    case Seq(_: CypherNode, tail@_*) if nextIsNode.maybeTrue => isPathLike(tail, False)
    case Seq(_: CypherRelationship, tail@_*) if nextIsNode.maybeFalse => isPathLike(tail, True)
    case Seq() => nextIsNode.isDefinite
    case _ => false
  }
}

