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
package org.opencypher.spark.impl.record

import cats.{Eval, Monad}
import cats.data.{State, StateT}
import cats.data.State.{get, set}
import cats.instances.all._
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.expr.{Expr, HasLabel, Property, Var}
import org.opencypher.spark.api.record._
import org.opencypher.spark.impl.spark.SparkColumnName
import org.opencypher.spark.impl.syntax.expr._
import org.opencypher.spark.impl.syntax.register._
import org.opencypher.spark.impl.util.RefCollection.AbstractRegister
import org.opencypher.spark.impl.util._

import scala.annotation.tailrec

// TODO: Prevent projection of expressions with unfulfilled dependencies
final case class InternalHeader protected[spark](
    private val slotContents: RefCollection[SlotContent],
    private val exprSlots: Map[Expr, Vector[Int]],
    private val cachedFields: Set[Var]
  )
  extends Serializable {

  self =>

  import InternalHeader.{addContent, recordSlotRegister}

  private lazy val cachedSlots = slotContents.contents.map(RecordSlot.from).toIndexedSeq
  private lazy val cachedColumns = slots.map(computeColumnName).toVector

  def ++(other: InternalHeader): InternalHeader =
    other.slotContents.elts.foldLeft(this) {
      case (acc, content) => acc + content
    }

  def slots: IndexedSeq[RecordSlot] = cachedSlots
  def fields: Set[Var] = cachedFields

  def slotsByName(name: String): Seq[RecordSlot] = {
    val filtered = exprSlots.filterKeys {
      case inner: Var => inner.name == name
      case Property(v: Var, _) => v.name == name
      case HasLabel(v: Var, _) => v.name == name
      case _ => false
    }
    filtered.values.headOption.getOrElse(Vector.empty).flatMap(ref => slotContents.lookup(ref).map(RecordSlot(ref, _)))
  }

  def slotsFor(expr: Expr): Seq[RecordSlot] =
    exprSlots.getOrElse(expr, Vector.empty).flatMap(ref => slotContents.lookup(ref).map(RecordSlot(ref, _)))

  def +(addedContent: SlotContent): InternalHeader =
    addContent(addedContent).runS(self).value

  def columns = cachedColumns

  def column(slot: RecordSlot) = cachedColumns(slot.index)

  def mandatory(slot: RecordSlot) = slot.content match {
    case _: FieldSlotContent => slot.content.cypherType.isMaterial
    case p@ProjectedExpr(expr) => p.cypherType.isMaterial && slotsFor(expr).size <=1
  }

  private def computeColumnName(slot: RecordSlot): String = SparkColumnName.of(slot)
}

object InternalHeader {

  private type HeaderState[X] = State[InternalHeader, X]

  val empty = new InternalHeader(RefCollection.empty, Map.empty, Set.empty)

  def apply(contents: SlotContent*) =
    from(contents)

  def from(contents: TraversableOnce[SlotContent]) =
    contents.foldLeft(empty) { case (header, slot) => header + slot }

  def addContents(contents: Seq[SlotContent]): State[InternalHeader, Vector[AdditiveUpdateResult[RecordSlot]]] =
    execAll(contents.map(addContent).toVector)

  def addContent(addedContent: SlotContent): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    addedContent match {
      case (it: ProjectedExpr) => addProjectedExpr(it)
      case (it: OpaqueField) => addOpaqueField(it)
      case (it: ProjectedField) => addProjectedField(it)
    }

  private def addProjectedExpr(content: ProjectedExpr): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    for (
      header <- get[InternalHeader];
      result <- {
        val existingSlot =
          for (slot <- header.slotsFor(content.expr).headOption)
          yield pureState[AdditiveUpdateResult[RecordSlot]](Found(slot))
        existingSlot.getOrElse {
            header.slotContents.insert(content) match {
              case Left(ref) => pureState(Found(slot(header, ref)))
              case Right((optNewSlots, ref)) => addSlotContent(optNewSlots, ref, content)
            }
          }
      }
    )
    yield result

  private def addOpaqueField(addedContent: OpaqueField): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] = {
    for {
      header <- get[InternalHeader]
      result <- {
        val existingSlots = header.slotsByName(addedContent.field.name)
        val replacement = existingSlots.headOption.flatMap[State[InternalHeader, AdditiveUpdateResult[RecordSlot]]] {
          case RecordSlot(ref, f: OpaqueField) if f == addedContent && f.cypherType == addedContent.cypherType =>
            Some(pureState(Found(slot(header, ref))))
          case RecordSlot(ref, _: OpaqueField) =>
            Some(header.slotContents.update(ref, addedContent) match {
              case Left(conflict) =>
                pureState(FailedToAdd(slot(header, conflict), Added(RecordSlot(ref, addedContent))))
              case Right(newSlots) =>
                addSlotContent(Some(newSlots), ref, addedContent).map(added => Replaced(slot(header, ref), added.it))
            })
          case _ => None
        }

        replacement.getOrElse(addField(addedContent))
      }
    } yield result
  }

  private def addProjectedField(addedContent: ProjectedField): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    for(
      header <- get[InternalHeader];
      result <- {
        val existingSlot = header.slotsFor(addedContent.expr).headOption
        existingSlot.flatMap[State[InternalHeader, AdditiveUpdateResult[RecordSlot]]] {
          case RecordSlot(ref, _: ProjectedExpr) =>
            Some(header.slotContents.update(ref, addedContent) match {
              case Left(conflict) =>
                pureState(FailedToAdd(slot(header, conflict), Added(RecordSlot(ref, addedContent))))

              case Right(newSlots) =>
                addSlotContent(Some(newSlots), ref, addedContent).map(added => Replaced(slot(header, ref), added.it))
            })
          case _ =>
            None
        }
        .getOrElse { addField(addedContent) }
      }
    )
    yield result

  private def addField(addedContent: FieldSlotContent): State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    for (
      header <- get[InternalHeader];
      result <- {
        header.slotContents.insert(addedContent) match {
          case Left(ref) => pureState(FailedToAdd(slot(header, ref), Added(RecordSlot(ref, addedContent))))
          case Right((optNewSlots, ref)) => addSlotContent(optNewSlots, ref, addedContent)
        }
      }
    )
    yield result


  private def addSlotContent(optNewSlots: Option[RefCollection[SlotContent]], ref: Int, addedContent: SlotContent)
  : State[InternalHeader, AdditiveUpdateResult[RecordSlot]] =
    for (
      header <- get[InternalHeader];
      result <-
        optNewSlots match {
          case Some(newSlots) =>
            val newExprSlots = addedContent.support.foldLeft(header.exprSlots) {
              case (slots, expr) => addExprSlots(slots, expr, ref)
            }
            val newFields = addedContent.alias.map { alias =>
              (header.cachedFields + alias).map {
                case v if v.name == alias.name => alias
                case v => v
              }
            }.getOrElse(header.cachedFields)
            val newHeader = InternalHeader(newSlots, newExprSlots, newFields)
            set[InternalHeader](newHeader).map(_ => Added(RecordSlot(ref, addedContent)))

          case None =>
            pureState(Found(slot(header, ref)))
        }
    )
    yield result

  private def addExprSlots(m: Map[Expr, Vector[Int]], key: Expr, value: Int): Map[Expr, Vector[Int]] =
    if (m.getOrElse(key, Vector.empty).contains(value)) m else m.updated(key, m.getOrElse(key, Vector.empty) :+ value)

  def compactFields : State[InternalHeader, Vector[RemovingUpdateResult[RecordSlot]]] =
    selectFields {
      case RecordSlot(_, content: ProjectedExpr) => content.owner.nonEmpty
      case _ => true
    }

  def selectFields(predicate: RecordSlot => Boolean)
  : State[InternalHeader, Vector[RemovingUpdateResult[RecordSlot]]] =
    get[InternalHeader].flatMap { header =>
      val toBeRemoved = header.slots.filterNot(predicate)
      val removals = toBeRemoved.map(slot => removeContent(slot)).toVector
      execAll(removals)
    }

  private def removeContent(removedSlot: RecordSlot)
  : State[InternalHeader, RemovingUpdateResult[RecordSlot]] = {
    get[InternalHeader].flatMap { header =>
      header.slotContents.find(removedSlot.content) match {
        case Some(ref) if ref == removedSlot.index =>
          val (remainingSlots, removedSlots) = removeDependencies(List(List(removedSlot)), header.slots.toSet)
          val remainingSlotsInOrder = remainingSlots.toSeq.sortBy(_.index)
          addContents(remainingSlotsInOrder.map(_.content)).map { _ =>
            Removed(removedSlot, removedSlots - removedSlot)
          }

        case _ =>
          pureState(NotFound(removedSlot))
      }
    }
  }

  @tailrec
  private def removeDependencies(
    drop: List[List[RecordSlot]],
    remaining: Set[RecordSlot],
    removedFields: Set[Var] = Set.empty,
    removedSlots: Set[RecordSlot] = Set.empty
  )
  : (Set[RecordSlot], Set[RecordSlot]) =
    drop match {
      case (hdList: List[RecordSlot]) :: (tlList: List[List[RecordSlot]]) =>
        hdList match {
          case hd :: tl if !removedSlots.contains(hd) =>
            hd.content match {
              case s: FieldSlotContent =>
                val newRemovedFields = removedFields + s.field
                val (nonDepending, depending) = remaining.partition {
                  // a slot is a dependency of itself
                  case slot if slot == hd => false
                  case RecordSlot(_, c: ProjectedSlotContent) => (c.expr.dependencies intersect newRemovedFields).isEmpty
                  case _ => true
                }
                val newRemoved = depending.toList :: tlList
                removeDependencies(newRemoved, nonDepending, newRemovedFields, removedSlots + hd)
              case _ =>
                removeDependencies(tlList, remaining - hd, removedFields, removedSlots + hd)
            }
          case _ =>
            removeDependencies(tlList, remaining, removedFields, removedSlots)
        }
      case _ =>
        remaining -> removedSlots
    }

  private def pureState[X](it: X) = State.pure[InternalHeader, X](it)

  private implicit def recordSlotRegister: AbstractRegister[Int, (Expr, CypherType), SlotContent] =
    new AbstractRegister[Int, (Expr, CypherType), SlotContent]() {
      override def key(defn: SlotContent): (Expr, CypherType) = defn.key -> defn.cypherType
      override protected def id(ref: Int): Int = ref
      override protected def ref(id: Int): Int = id
    }

  private def slot(header: InternalHeader, ref: Int) = RecordSlot(ref, header.slotContents.elts(ref))

  private def execAll[O](input: Vector[State[InternalHeader, O]]): State[InternalHeader, Vector[O]] =
    Monad[HeaderState].sequence(input)
}
