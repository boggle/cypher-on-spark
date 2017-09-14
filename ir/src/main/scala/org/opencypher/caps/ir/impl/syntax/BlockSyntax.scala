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
package org.opencypher.caps.ir.impl.syntax

import org.opencypher.caps.ir.api.IRField
import org.opencypher.caps.ir.api.block.Block
import org.opencypher.caps.ir.classes.TypedBlock

import scala.language.implicitConversions

trait BlockSyntax {
  implicit def typedBlockOps[B <: Block[_], E](block: B)(implicit instance: TypedBlock[B] { type BlockExpr = E })
  : TypedBlockOps[B, E] =
    new TypedBlockOps[B, E](block)
}

final class TypedBlockOps[B <: Block[_], E](block: B)(implicit instance: TypedBlock[B] { type BlockExpr = E }) {
  def outputs: Set[IRField] = instance.outputs(block)
}
