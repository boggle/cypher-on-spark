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

import org.opencypher.spark_legacy.api.frame.BinaryRepresentation
import org.opencypher.spark.api.types.{CTList, CTNode, CTRelationship}
import org.opencypher.spark.api.value.CypherList

class VarExpandTest extends StdFrameTestSuite {

  test("var expand 1-2 length") {
    val n1 = add(newNode)
    val n2 = add(newNode)
    val n3 = add(newNode)
    val n4 = add(newNode)

    val r1 = add(newUntypedRelationship(n1, n2))
    val r2 = add(newUntypedRelationship(n2, n3))
    val r3 = add(newUntypedRelationship(n3, n4))

    new GraphTest {
      import frames._

      val result = allNodes('a).asProduct.varExpand('a, 1, 2)('r).testResult

      result.signature shouldHaveFields('a -> CTNode, 'r -> CTList(CTRelationship))
      result.signature shouldHaveFieldSlots('a -> BinaryRepresentation, 'r -> BinaryRepresentation)
      result.toSet should equal(Set(n1 -> CypherList(Seq(r1)),
                                    n1 -> CypherList(Seq(r1, r2)),
                                    n2 -> CypherList(Seq(r2)),
                                    n2 -> CypherList(Seq(r2, r3)),
                                    n3 -> CypherList(Seq(r3))))
    }
  }

  test("var expand 2-3 length") {
    val n1 = add(newNode)
    val n2 = add(newNode)
    val n3 = add(newNode)
    val n4 = add(newNode)

    val r1 = add(newUntypedRelationship(n1, n2))
    val r2 = add(newUntypedRelationship(n2, n3))
    val r3 = add(newUntypedRelationship(n3, n4))

    new GraphTest {
      import frames._

      val result = allNodes('a).asProduct.varExpand('a, 2, 3)('r).testResult

      result.signature shouldHaveFields('a -> CTNode, 'r -> CTList(CTRelationship))
      result.signature shouldHaveFieldSlots('a -> BinaryRepresentation, 'r -> BinaryRepresentation)
      result.toSet should equal(Set(n1 -> CypherList(Seq(r1, r2)),
                                    n1 -> CypherList(Seq(r1, r2, r3)),
                                    n2 -> CypherList(Seq(r2, r3))
                                    ))
    }
  }

  test("var expand 0-1 length") {
    val n1 = add(newNode)
    val n2 = add(newNode)
    val n3 = add(newNode)
    val n4 = add(newNode)

    val r1 = add(newUntypedRelationship(n1, n2))
    val r2 = add(newUntypedRelationship(n2, n3))
    val r3 = add(newUntypedRelationship(n3, n4))

    new GraphTest {
      import frames._

      val result = allNodes('a).asProduct.varExpand('a, 0, 1)('r).testResult

      result.signature shouldHaveFields('a -> CTNode, 'r -> CTList(CTRelationship))
      result.signature shouldHaveFieldSlots('a -> BinaryRepresentation, 'r -> BinaryRepresentation)
      result.toSet should equal(Set(n1 -> CypherList(Seq(r1)),
                                    n1 -> CypherList(Seq()),
                                    n2 -> CypherList(Seq()),
                                    n2 -> CypherList(Seq(r2)),
                                    n3 -> CypherList(Seq()),
                                    n3 -> CypherList(Seq(r3)),
                                    n4 -> CypherList(Seq())
                                    ))
    }
  }

  test("var expand should not repeat relationships") {
    val n1 = add(newNode)
    val r1 = add(newUntypedRelationship(n1, n1))

    new GraphTest {
      import frames._

      val result = allNodes('a).asProduct.varExpand('a, 1, 3)('r).testResult

      result.signature shouldHaveFields('a -> CTNode, 'r -> CTList(CTRelationship))
      result.signature shouldHaveFieldSlots('a -> BinaryRepresentation, 'r -> BinaryRepresentation)
      result.toSet should equal(Set(n1 -> CypherList(Seq(r1))))
    }
  }

}
