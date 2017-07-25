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
package org.opencypher.spark.api.spark

import java.util.Collections

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.opencypher.spark.api.expr.{Property, Var}
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.types.{CTList, CTNode, CTRelationship, CypherType}
import org.opencypher.spark.api.value.{CypherMap, CypherValue}
import org.opencypher.spark.impl.convert.{fromSparkType, toSparkType}
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.record.SparkCypherRecordHeader
import org.opencypher.spark.impl.spark.SparkColumnName
import org.opencypher.spark.impl.syntax.header._

import scala.annotation.tailrec
import scala.reflect.runtime.universe.TypeTag

sealed abstract class SparkCypherRecords(tokens: SparkCypherTokens,
                                         initialHeader: RecordHeader,
                                         initialData: DataFrame,
                                         optDetailedRecords: Option[SparkCypherRecords])
                                        (implicit val space: SparkGraphSpace)
  extends CypherRecords with Serializable {

  self =>

  override type Data = DataFrame
  override type Records = SparkCypherRecords

  override def header = initialHeader
  override def data = initialData

  override def columns: IndexedSeq[String] =
    header.internalHeader.columns

  override def column(slot: RecordSlot): String =
    header.internalHeader.column(slot)

  //noinspection AccessorLikeMethodIsEmptyParen
  def toDF(): Data = data

  def mapDF(f: Data => Data) = SparkCypherRecords.create(f(data))

  // TODO: Check that this does not change the caching of our data frame
  def cached = SparkCypherRecords.create(header, data.cache())

  override def show() = RecordsPrinter.print(this)

  def details = optDetailedRecords.getOrElse(this)

  def compact = {
    val cachedHeader = self.header.update(compactFields)._1
    val cachedData = {
      val columns = cachedHeader.slots.map(c => new Column(SparkColumnName.of(c.content)))
      self.data.select(columns: _*)
    }

    SparkCypherRecords.create(cachedHeader, cachedData)
  }

  // only keep slots with v as their owner
  //  def focus(v: Var): SparkCypherRecords = {
  //    val (newHeader, _) = self.header.update(selectFields(slot => slot.content.owner.contains(v)))
  //    val newColumns = newHeader.slots.collect {
  //      case RecordSlot(_, content: FieldSlotContent) => new Column(SparkColumnName.of(content))
  //    }
  //    new SparkCypherRecords {
  //      override def header = newHeader
  //      override def data = self.data.select(newColumns: _*)
  //    }
  //  }

  //  // alias oldVar to newVar, without guarding against shadowing
  //  def alias(oldVar: Var, newVar: Var): SparkCypherRecords = {
  //    val oldIndices: Map[SlotContent, Int] = self.header.slots.map { slot: RecordSlot =>
  //      slot.content match {
  //        case p: ProjectedSlotContent =>
  //          p.expr match {
  //            case h@HasLabel(`oldVar`, label) => ProjectedExpr(HasLabel(newVar, label)(h.cypherType)) -> slot.index
  //            case p@Property(`oldVar`, key) => ProjectedExpr(Property(newVar, key)(p.cypherType))-> slot.index
  //            case _ => p -> slot.index
  //          }
  //
  //        case _: OpaqueField => OpaqueField(newVar) -> slot.index
  //        case content => content -> slot.index
  //      }
  //    }.toMap
  //
  //    // TODO: Check result for failure to add
  //    val (newHeader, _) = RecordHeader.empty.update(addContents(oldIndices.keySet.toSeq))
  //    val newIndices = newHeader.slots.map(slot => slot.content -> slot.index).toMap
  //    val indexMapping = oldIndices.map {
  //      case (content, oldIndex) => oldIndex -> newIndices(content)
  //    }.toSeq.sortBy(_._2)
  //
  //    val columns = indexMapping.map {
  //      case (oldIndex, newIndex) =>
  //        val oldName = SparkColumnName.of(self.header.slots(oldIndex).content)
  //        val newName = SparkColumnName.of(newHeader.slots(newIndex).content)
  //        new Column(oldName).as(newName)
  //    }
  //
  //    val newData = self.data.select(columns: _*)
  //
  //    new SparkCypherRecords {
  //      override def data = newData
  //      override def header = newHeader
  //    }
  //  }
  //

  //  // union two record sets in their shared columns, dropping all non-shared columns
  //  // missing values, but discarding overlapping slots
  //  def union(other: SparkCypherRecords): SparkCypherRecords = {
  //    val shared = (self.header.slots intersect other.header.slots).map(_.content).toSet
  //    val contents = (self.header.slots ++ other.header.slots).map(_.content).filter(content => shared(content)).distinct
  //    val (newHeader, _) = RecordHeader.empty.update(addContents(contents))
  //
  //    val newColumns = self.header.slots.collect { case slot if shared(slot.content) => new Column(SparkColumnName.of(slot.content)) }
  //
  //    val selfData = self.data.select(newColumns: _*)
  //    val otherData = other.data.select(newColumns: _*)
  //
  //    // TODO: Make distinct per entity fields
  //    val newData = selfData.union(otherData).distinct()
  //    new SparkCypherRecords {
  //      override def header = newHeader
  //      override def data = newData
  //    }
  //  }

  //  def intersect(other: SparkCypherRecords): SparkCypherRecords = {
  //    val shared = (self.header.slots intersect other.header.slots).map(_.content).toSet
  //    val contents = (self.header.slots ++ other.header.slots).map(_.content).filter(content => shared(content)).distinct
  //    val (newHeader, _) = RecordHeader.empty.update(addContents(contents))
  //
  //    val newColumns = self.header.slots.collect { case slot if shared(slot.content) => new Column(SparkColumnName.of(slot.content)) }
  //
  //    val selfData = self.data.select(newColumns: _*)
  //    val otherData = other.data.select(newColumns: _*)
  //
  //    // TODO: Make distinct per entity fields
  //    val newData = selfData.intersect(otherData).distinct()
  //
  //    new SparkCypherRecords {
  //      override def header = newHeader
  //      override def data = newData
  //    }
  //  }

  //  // concatenates two record sets, using a union of their columns and using null as as default for
  //  // missing values, but discarding overlapping slots
  //  def concat(other: SparkCypherRecords): SparkCypherRecords = {
  //    val duplicate = (self.header.slots intersect other.header.slots).map(_.content).toSet
  //    val contents = (self.header.slots ++ other.header.slots).map(_.content).filter(content => !duplicate(content)).distinct
  //    val (newHeader, _) = RecordHeader.empty.update(addContents(contents))
  //
  //    val selfColumns =
  //      self.header.slots.collect { case slot if !duplicate(slot.content) => new Column(SparkColumnName.of(slot.content))} ++
  //      other.header.slots.collect { case slot if !duplicate(slot.content) => new Column(Literal(null, toSparkType(slot.content.cypherType))).as(SparkColumnName.of(slot.content)) }
  //    val newSelfData = self.data.select(selfColumns: _*)
  //
  //    val otherColumns =
  //      self.header.slots.collect { case slot if !duplicate(slot.content) => new Column(Literal(null, toSparkType(slot.content.cypherType))).as(SparkColumnName.of(slot.content)) } ++
  //      other.header.slots.collect { case slot if !duplicate(slot.content) => new Column(SparkColumnName.of(slot.content))}
  //    val newOtherData = other.data.select(otherColumns: _*)
  //
  //    new SparkCypherRecords {
  //      override def header = newHeader
  //      override def data = newSelfData.union(newOtherData)
  //    }
  //  }

  override def contract[E <: EmbeddedEntity](entity: VerifiedEmbeddedEntity[E]): SparkCypherRecords = {
    val slotExprs = entity.slots
    val newSlots = header.slots.map {
      case slot@RecordSlot(idx, content: FieldSlotContent) =>
        slotExprs.get(content.field.name).map {
          case expr: Var      => OpaqueField(expr)
          case expr: Property => ProjectedExpr(expr.copy()(content.cypherType))
          case expr           => ProjectedExpr(expr)
        }
        .getOrElse(slot.content)

      case slot =>
        slot.content
    }
    val newHeader = RecordHeader.from(newSlots: _*)
    val renamed = data.toDF(newHeader.internalHeader.columns: _*)
    SparkCypherRecords.create(newHeader, renamed)
  }

  def distinct: SparkCypherRecords = SparkCypherRecords.create(self.header, self.data.distinct())

  def toLocalIterator: java.util.Iterator[CypherMap] = {
    val iterator = data.toLocalIterator()
    new java.util.Iterator[CypherMap] {
      override def next(): CypherMap = {
        val it = iterator.next()
        val entries = columns.map { (column) =>
          val fieldIndex = it.fieldIndex(column)
          val javaValue = it.get(fieldIndex)
          val scalaValue = CypherValue(javaValue)
          column -> scalaValue
        }
        CypherMap(entries: _*)
      }

      override def hasNext: Boolean = iterator.hasNext
    }
  }

  def toScalaIterator: Iterator[CypherMap] = {
    val iterator = data.toLocalIterator()
    new Iterator[CypherMap] {
      override def next(): CypherMap = {
        val it = iterator.next()
        val entries = columns.map { (column) =>
          val fieldIndex = it.fieldIndex(column)
          val javaValue = it.get(fieldIndex)
          val scalaValue = CypherValue(javaValue)
          column -> scalaValue
        }
        CypherMap(entries: _*)
      }

      override def hasNext: Boolean = iterator.hasNext
    }
  }
}

object SparkCypherRecords {

  def create[A <: Product : TypeTag](columns: Seq[String], data: Seq[A])(implicit graphSpace: SparkGraphSpace)
  : SparkCypherRecords =
    create(graphSpace.session.createDataFrame(data).toDF(columns: _*))

  def create[A <: Product : TypeTag](data: Seq[A])(implicit graphSpace: SparkGraphSpace)
  : SparkCypherRecords =
    create(graphSpace.session.createDataFrame(data))

  def create(columns: String*)(rows: java.util.List[Row], schema: StructType)(implicit graphSpace: SparkGraphSpace)
  : SparkCypherRecords =
    create(graphSpace.session.createDataFrame(rows, schema).toDF(columns: _*))

  def create(rows: java.util.List[Row], schema: StructType)(implicit graphSpace: SparkGraphSpace): SparkCypherRecords =
    create(graphSpace.session.createDataFrame(rows, schema))

  def create(columns: Seq[String], data: java.util.List[_], beanClass: Class[_])(implicit graphSpace: SparkGraphSpace)
  : SparkCypherRecords =
    create(graphSpace.session.createDataFrame(data, beanClass).toDF(columns: _*))

  def create(data: java.util.List[_], beanClass: Class[_])(implicit graphSpace: SparkGraphSpace): SparkCypherRecords =
    create(graphSpace.session.createDataFrame(data, beanClass))

  def create[A <: Product : TypeTag](rdd: RDD[A])(implicit graphSpace: SparkGraphSpace): SparkCypherRecords =
    create(graphSpace.session.createDataFrame(rdd))

  def create(rowRDD: RDD[Row], schema: StructType)(implicit graphSpace: SparkGraphSpace): SparkCypherRecords =
    create(graphSpace.session.createDataFrame(rowRDD, schema))

  def create(rowRDD: JavaRDD[Row], schema: StructType)(implicit graphSpace: SparkGraphSpace): SparkCypherRecords =
    create(graphSpace.session.createDataFrame(rowRDD, schema))

  def create(rdd: RDD[_], beanClass: Class[_])(implicit graphSpace: SparkGraphSpace): SparkCypherRecords =
    create(graphSpace.session.createDataFrame(rdd, beanClass))

  def create(rdd: JavaRDD[_], beanClass: Class[_])(implicit graphSpace: SparkGraphSpace): SparkCypherRecords =
    create(graphSpace.session.createDataFrame(rdd, beanClass))

  def create(initialDataFrame: DataFrame)(implicit graphSpace: SparkGraphSpace): SparkCypherRecords = {
    val initialHeader = SparkCypherRecordHeader.fromSparkStructType(initialDataFrame.schema)

    // rename data to match generated header
    // we trust the order of the generated header
    val renamed = initialDataFrame.toDF(initialHeader.internalHeader.columns: _*)

    create(initialHeader, renamed)
  }

  /**
    * This does not mandate that the <i>order</i> of the RecordHeader and the DataFrame are aligned, as long as the
    * <i>names</i> are the same (and not duplicated).
    *
    * @param initialHeader the header of the records.
    * @param initialData the data of the records.
    * @param graphSpace the space in which the data belongs.
    * @return a new SparkCypherRecords representing the input.
    */
  def create(initialHeader: RecordHeader, initialData: DataFrame)(implicit graphSpace: SparkGraphSpace)
  : SparkCypherRecords = {
    if (initialData.sparkSession == graphSpace.session) {

      // Ensure no duplicate columns in initialData
      val initialDataColumns = initialData.columns.toSeq
      if (initialDataColumns.size != initialDataColumns.distinct.size)
        Raise.duplicateColumnNamesInData()

      // Verify correct column names
      if (initialData.columns.toSet != initialHeader.internalHeader.columns.toSet)
        Raise.recordsDataHeaderMismatch()

      // Verify column types
      initialHeader.slots.foreach { slot =>
        val dfSchema = initialData.schema
        val field = dfSchema(SparkColumnName.of(slot))
        val cypherType = fromSparkType(field.dataType, field.nullable)
        val headerType = slot.content.cypherType

        // if the type in the data doesn't correspond to the type in the header we fail
        // except: we encode nodes, rels and integers with the same data type, so we can't fail
        // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)
        if (toSparkType(headerType) != toSparkType(cypherType) && !containsEntity(headerType))
          Raise.invalidDataTypeForColumn(field.name, headerType.toString, cypherType.toString)
      }

      val internalRecords = createInternal(initialHeader, initialData, None)
      val isSanitized = initialHeader.slots.map(_.content).collectFirst { case _: ProjectedExpr => true }.isEmpty
      if (isSanitized) {
        internalRecords
      } else {
        val fieldContents = initialHeader.contents.collect { case content: FieldSlotContent => content }.toSeq
        val (sanitizedHeader, _) = RecordHeader.empty.update(addContents(fieldContents))
        val remainingColumnNames = sanitizedHeader.slots.map { s => SparkColumnName.of(s.content) }.toSet
        val existingColumns = initialData.columns
        val sanitizedColumns = existingColumns.filter(remainingColumnNames).map(initialData.col)
        val sanitizedData = initialData.select(sanitizedColumns: _*)
        createInternal(sanitizedHeader, sanitizedData, Some(internalRecords))
      }
    }
    else {
      Raise.graphSpaceMismatch()
    }
  }

  private def createInternal(header: RecordHeader, data: DataFrame, optRecordsWithDetails: Option[SparkCypherRecords])
                            (implicit graphSpace: SparkGraphSpace) =
    new SparkCypherRecords(graphSpace.tokens, header, data, optRecordsWithDetails) {}

  @tailrec
  private def containsEntity(t: CypherType): Boolean = t match {
    case _: CTNode => true
    case _: CTRelationship => true
    case l: CTList => containsEntity(l.elementType)
    case _ => false
  }

  def empty(initialHeader: RecordHeader = RecordHeader.empty)(implicit graphSpace: SparkGraphSpace)
  : SparkCypherRecords = {
    val initialSparkStructType = SparkCypherRecordHeader.asSparkStructType(initialHeader)
    val initialDataFrame = graphSpace.session.createDataFrame(Collections.emptyList[Row](), initialSparkStructType)
    create(initialHeader, initialDataFrame)
  }
}
