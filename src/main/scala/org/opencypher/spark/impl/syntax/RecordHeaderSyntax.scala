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
package org.opencypher.spark.impl.syntax

import cats.data.State
import cats.data.State.{get, set}
import org.opencypher.spark.api.record._
import org.opencypher.spark.impl.record.InternalHeader
import org.opencypher.spark.impl.util.{AdditiveUpdateResult, RemovingUpdateResult}

import scala.language.implicitConversions

trait RecordHeaderSyntax {

  implicit def sparkRecordHeaderSyntax(header: RecordHeader): RecordHeaderOps =
    new RecordHeaderOps(header)

  type HeaderState[X] = State[RecordHeader, X]

  def addContents(contents: Seq[SlotContent]): State[RecordHeader, Vector[AdditiveUpdateResult[RecordSlot]]] =
    exec(InternalHeader.addContents(contents))

  def addContent(content: SlotContent): State[RecordHeader, AdditiveUpdateResult[RecordSlot]] =
    exec(InternalHeader.addContent(content))

  def compactFields: State[RecordHeader, Vector[RemovingUpdateResult[RecordSlot]]] =
    exec(InternalHeader.compactFields)

//  def selectFields(predicate: RecordSlot => Boolean)
//  : State[RecordHeader, Vector[RemovingUpdateResult[RecordSlot]]] =
//    exec(InternalHeader.selectFields(predicate))

//  def removeContent(content: SlotContent)
//  : State[RecordHeader, RemovingUpdateResult[SlotContent]] =
//    exec(InternalHeader.removeContent(content))

  private def exec[O](inner: State[InternalHeader, O]): State[RecordHeader, O] =
    get[RecordHeader]
    .map(header => inner.run(header.internalHeader).value)
    .flatMap { case (newInternalHeader, value) => set(RecordHeader(newInternalHeader)).map(_ => value) }
}

final class RecordHeaderOps(header: RecordHeader) {

  def +(content: SlotContent): RecordHeader =
    header.copy(header.internalHeader + content)

  def update[A](ops: State[RecordHeader, A]): (RecordHeader, A) =
    ops.run(header).value
}
