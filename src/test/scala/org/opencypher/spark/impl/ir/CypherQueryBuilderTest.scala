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
package org.opencypher.spark.impl.ir

import org.opencypher.spark.api.expr.{Expr, HasLabel}
import org.opencypher.spark.api.ir.block._
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.ir.pattern._
import org.opencypher.spark.api.ir.{Field, QueryModel}
import org.opencypher.spark.api.types.CTNode
import org.opencypher.spark._

import scala.collection.immutable.Set

class CypherQueryBuilderTest extends IrTestSuite {

  test("match node and return it") {
    "MATCH (a:Person) RETURN a".model.ensureThat { (model, globals) =>

      import globals.tokens._
      import globals.constants._

      val loadRef = model.findExactlyOne {
        case NoWhereBlock(LoadGraphBlock(binds, DefaultGraph())) =>
          binds shouldBe empty
      }

      val matchRef = model.findExactlyOne {
        case MatchBlock(deps, Pattern(entities, topo), AllGiven(exprs), _) =>
          deps should equal(Set(loadRef))
          entities should equal(Map(toField('a, CTNode) -> EveryNode))
          topo shouldBe empty
          exprs should equal(Set(HasLabel(toVar('a), labelByName("Person"))()))
      }

      val projectRef = model.findExactlyOne {
        case NoWhereBlock(ProjectBlock(deps, ProjectedFields(map), _, _)) =>
          deps should equal(Set(matchRef))
          map should equal(Map(toField('a) -> toVar('a)))
      }

      model.result match {
        case NoWhereBlock(ResultBlock(deps, FieldsInOrder(Field("a")), _, _, _, _)) =>
          deps should equal(Set(projectRef))
      }

      model.requirements should equal(Map(
        projectRef -> Set(matchRef),
        matchRef -> Set(loadRef),
        loadRef -> Set()
      ))
    }
  }

  test("match simple relationship pattern and return some fields") {
    "MATCH (a)-[r]->(b) RETURN b AS otherB, a, r".model.ensureThat { (model, globals) =>

      val loadRef = model.findExactlyOne {
        case NoWhereBlock(LoadGraphBlock(binds, DefaultGraph())) =>
          binds shouldBe empty
      }

      val matchRef = model.findExactlyOne {
        case NoWhereBlock(MatchBlock(deps, Pattern(entities, topo), _, _)) =>
          deps should equal(Set(loadRef))
          entities should equal(Map(toField('a) -> EveryNode, toField('b) -> EveryNode, toField('r) -> EveryRelationship))
          val map = Map(toField('r) -> DirectedRelationship('a, 'b))
          topo should equal(map)
      }

      val projectRef = model.findExactlyOne {
        case NoWhereBlock(ProjectBlock(deps, ProjectedFields(map), _, _)) =>
          deps should equal(Set(matchRef))
          map should equal(Map(
            toField('a) -> toVar('a),
            toField('otherB) -> toVar('b),
            toField('r) -> toVar('r)
          ))
      }

      model.result match {
        case NoWhereBlock(ResultBlock(_, FieldsInOrder(Field("otherB"), Field("a"), Field("r")), _, _, _, _)) =>
      }

      model.requirements should equal(Map(
        projectRef -> Set(matchRef),
        matchRef -> Set(loadRef),
        loadRef -> Set()
      ))
    }
  }

  implicit class RichModel(model: QueryModel[Expr]) {

    def ensureThat(f: (QueryModel[Expr], GlobalsRegistry) => Unit) = f(model, model.globals)

    def requirements = {
      val deps = model.result.after
      val allDeps = deps.flatMap(model.allDependencies) ++ deps
      model.blocks.keySet should equal(allDeps)
      allDeps.map { ref => ref -> model.dependencies(ref) }.toMap
    }

    def findExactlyOne(f: PartialFunction[Block[Expr], Unit]): BlockRef = {
      val result = model.collect {
        case (ref, block) if f.isDefinedAt(block) =>
          f(block)
          ref
      }
      withClue(s"Failed to extract single matching block from ${model.blocks}") {
        result.size should equal(1)
      }
      result.head
    }
  }
}
