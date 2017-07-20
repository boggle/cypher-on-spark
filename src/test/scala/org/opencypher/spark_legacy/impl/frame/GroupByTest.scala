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

import org.apache.spark.sql.types.{IntegerType, LongType}
import org.opencypher.spark_legacy.api.frame.{BinaryRepresentation, EmbeddedRepresentation}
import org.opencypher.spark.api.types.{CTAny, CTInteger, CTList, CTNode}
import org.opencypher.spark.api.value.{CypherList, CypherString}
import org.scalatest.FunSuite

class GroupByTest extends StdFrameTestSuite {

  test("collect with simple unique grouping key") {
    val n1 = add(newNode)
    val n2 = add(newNode)
    val n3 = add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.groupBy('n)(Collect('n)('list)).testResult

      result.signature shouldHaveFields('n -> CTNode, 'list -> CTList(CTNode))
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'list -> BinaryRepresentation)
      result.toSet should equal(Set(n1 -> CypherList(Seq(n1)), n2 -> CypherList(Seq(n2)), n3 -> CypherList(Seq(n3))))
    }
  }

  test("collect should ignore null") {
    val n1 = add(newNode.withProperties("country" -> "Sweden"))
    val n2 = add(newNode)
    val n3 = add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'country)('country).groupBy()(Collect('country)('list)).testResult

      result.signature shouldHaveFields('list -> CTList(CTAny.nullable))
      result.signature shouldHaveFieldSlots('list -> BinaryRepresentation)
      result.toSet should equal(Set(Tuple1(CypherList(Seq("Sweden")))))
    }
  }

  test("count with simple unique grouping key") {
    val n1 = add(newNode)
    val n2 = add(newNode)
    val n3 = add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.groupBy('n)(Count('n)('count)).testResult

      result.signature shouldHaveFields('n -> CTNode, 'count -> CTInteger)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'count -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(n1 -> 1, n2 -> 1, n3 -> 1))
    }
  }

  test("count with grouping grouping key") {
    add(newNode.withProperties("country" -> "Sweden"))
    add(newNode.withProperties("country" -> "Sweden"))
    add(newNode.withProperties("country" -> "Germany"))
    add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'country)('country).groupBy('country)(Count('n)('count)).testResult

      result.signature shouldHaveFields('country -> CTAny.nullable, 'count -> CTInteger)
      result.signature shouldHaveFieldSlots('count -> EmbeddedRepresentation(LongType), 'country -> BinaryRepresentation)
      result.toSet should equal(Set(CypherString("Sweden") -> 2, CypherString("Germany") -> 1, (null, 1)))
    }
  }

  test("aggregate with composite grouping key") {
    val n1 = add(newNode.withProperties("country" -> "Sweden"))
    val n2 = add(newNode)
    val n3 = add(newNode.withProperties("country" -> "Germany"))

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.propertyValue('n, 'country)('country).groupBy('n, 'country)(Count('n)('count)).testResult

      result.signature shouldHaveFields('n -> CTNode, 'country -> CTAny.nullable, 'count -> CTInteger)
      result.signature shouldHaveFieldSlots('n -> BinaryRepresentation, 'count -> EmbeddedRepresentation(LongType), 'country -> BinaryRepresentation)
      result.toSet should equal(Set((n1, CypherString("Sweden"), 1), (n3, CypherString("Germany"), 1), (n2, null, 1)))
    }
  }

  test("count with empty grouping key") {
    add(newNode)
    add(newNode)
    add(newNode)

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.groupBy()(Count('n)('count)).testResult

      result.signature shouldHaveFields('count -> CTInteger)
      result.signature shouldHaveFieldSlots('count -> EmbeddedRepresentation(LongType))
      result.toSet should equal(Set(Tuple1(3)))
    }
  }
}
