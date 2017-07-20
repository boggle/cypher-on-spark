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

import org.apache.spark.sql.types.LongType
import org.opencypher.spark.api.types.{CTInteger, CTNode}
import org.opencypher.spark_legacy.api.frame.{BinaryRepresentation, EmbeddedRepresentation}
import org.opencypher.spark.api.value.CypherNode

class JoinTest extends StdFrameTestSuite {

  test("optional join") {
    val n1 = add(newNode.withLabels("A", "B"))
    val n2 = add(newNode.withLabels("A", "B"))
    val n3 = add(newNode.withLabels("A"))
    add(newNode.withLabels("B"))
    add(newNode)

    new GraphTest {
      import frames._

      val a = allNodes('a).labelFilter("A").asProduct.nodeId('a)('aid).asRow
      val b = allNodes('b).labelFilter("B").asProduct.nodeId('b)('bid).asRow

      val result = a.optionalJoin(b).on('aid)('bid).asProduct.testResult

      result.signature shouldHaveFields('a -> CTNode, 'aid -> CTInteger, 'b -> CTNode, 'bid -> CTInteger)
      result.signature shouldHaveFieldSlots(
        'a -> BinaryRepresentation,
        'aid -> EmbeddedRepresentation(LongType),
        'b -> BinaryRepresentation,
        'bid -> EmbeddedRepresentation(LongType))
      // TODO: Representation again; null value of primitive int is -1 (something with nullable columns)
      result.toSet should equal(Set((n1, 1, n1, 1), (n2, 2, n2, 2), (n3, 3, null, -1)))
    }
  }

  test("Joins on node ids") {
    add(newLabeledNode("A"))
    val n = add(newLabeledNode("A", "B"))
    val nid = CypherNode.id(n).get.v
    add(newLabeledNode("B"))

    new GraphTest {
      import frames._

      val lhs = allNodes('l).labelFilter("A").asProduct.nodeId('l)('lid).asRow
      val rhs = allNodes('r).labelFilter("B").asProduct.nodeId('r)('rid).asRow

      val result = lhs.join(rhs).on('lid)('rid).asProduct.testResult

      result.signature shouldHaveFields('l -> CTNode, 'lid -> CTInteger, 'r -> CTNode, 'rid -> CTInteger)
      result.signature shouldHaveFieldSlots(
        'l -> BinaryRepresentation,
        'lid -> EmbeddedRepresentation(LongType),
        'r -> BinaryRepresentation,
        'rid -> EmbeddedRepresentation(LongType)
      )
      result.toSet should equal(Set((n, nid, n, nid)))
    }
  }

  test("Refuses to join using non embedded slots") {
    new GraphTest {
      import frames._

      val lhs = allNodes('l).asRow
      val rhs = allNodes('r).asRow

      val error = the [FrameVerification.SlotNotEmbeddable] thrownBy {
        lhs.join(rhs).on('l)('r)
      }
      error.contextName should equal("requireEmbeddedRepresentation")
    }
  }

  test("Refuses to join on incompatible cypher types") {
    new GraphTest {
      import frames._

      val lhs = allNodes('l).asProduct.nodeId('l)('id).asRow
      val rhs = allRelationships('r).asProduct.relationshipType('r)('typ).asRow

      val error = the [FrameVerification.UnInhabitedMeetType] thrownBy {
        lhs.join(rhs).on('id)('typ)
      }
      error.contextName should equal("requireInhabitedMeetType")
    }
  }
}
