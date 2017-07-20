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
package org.opencypher.spark.api.ir

import org.opencypher.spark.api.expr.{Equals, Expr, Property}
import org.opencypher.spark.api.ir.block.{ProjectedFields, ProjectedFieldsOf}
import org.opencypher.spark.api.ir.pattern.{EveryNode, Pattern}
import org.opencypher.spark.api.types.CTBoolean
import org.opencypher.spark.impl.ir.IrTestSuite
import org.opencypher.spark.toField

class SolvedQueryModelTest extends IrTestSuite {

  test("add fields") {
    val s = SolvedQueryModel.empty.withField('a).withFields('b, 'c)

    s.fields should equal(Set('a, 'b, 'c).map(toField))
  }

  test("contains a block") {
    val block = matchBlock(Pattern.empty.withEntity('a, EveryNode).withEntity('b, EveryNode).withEntity('c, EveryNode))
    val s = SolvedQueryModel.empty[Expr].withField('a).withFields('b, 'c)

    s.contains(block) shouldBe true
  }

  test("contains several blocks") {
    val block1 = matchBlock(Pattern.empty.withEntity('a, EveryNode))
    val block2 = matchBlock(Pattern.empty.withEntity('b, EveryNode))
    val block3 = project(ProjectedFieldsOf[Expr](toField('c) -> Equals('a, 'b)(CTBoolean)))
    val block4 = project(ProjectedFieldsOf[Expr](toField('d) -> Equals('c, 'b)(CTBoolean)))

    val s = SolvedQueryModel.empty[Expr].withField('a).withFields('b, 'c)

    s.contains(block1) shouldBe true
    s.contains(block1, block2) shouldBe true
    s.contains(block1, block2, block3) shouldBe true
    s.contains(block1, block2, block3, block4) shouldBe false
  }

  test("solves") {
    val s = SolvedQueryModel.empty[Expr].withField('a).withFields('b, 'c)
    val p = Pattern.empty[Expr].withEntity('a, EveryNode).withEntity('b, EveryNode).withEntity('c, EveryNode)

    s.solves('a) shouldBe true
    s.solves('b) shouldBe true
    s.solves('x) shouldBe false
    s.solves(p) shouldBe true
    s.solves(p.withEntity('x, EveryNode)) shouldBe false
  }

}
