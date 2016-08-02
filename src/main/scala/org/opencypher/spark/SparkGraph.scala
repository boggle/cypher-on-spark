package org.opencypher.spark

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.serializer.{JavaSerializer, JavaSerializerInstance, KryoSerializer, KryoSerializerInstance}
import org.apache.spark.sql.types.{BinaryType, DataType, ObjectType, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast, Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.objects.{DecodeUsingSerializer, EncodeUsingSerializer}

import scala.reflect._


trait SparkGraph {

  /**
    * Each row contains a node.
    * Each node has three columns: id, labels, properties.
    * The id column is a long containing the node id.
    * The labels column is a list/set/??? containing the node's labels.
    * The properties column is a map from String to Any, containing the node's properties.
    */
  def nodes: Dataset[CypherNode]

  /**
    * Each row contains a relationship.
    * Each relationship has five columns: id, start, end, type, properties.
    * The id column is a long containing the relationship id.
    * The start column is a long containing the id of relationship's start node.
    * The end column is a long containing the id of relationship's end node.
    * The type column is a string containing the relationship type.
    * The properties column is a map from String to Any, containing the relationships's properties.
    */
  def relationships: Dataset[CypherRelationship]

  def sqlContext: SQLContext

}


case class SparkGraphImpl(nodes: Dataset[CypherNode], relationships: Dataset[CypherRelationship], sqlContext: SQLContext) extends SparkGraph {

}

trait CypherImplicits {
//  implicit def foo[]
  implicit def cypherValueEncoder[T <: CypherValue : ClassTag]: Encoder[T] = org.apache.spark.sql.Encoders.kryo[T]
//  ExpressionEncoder[T](
//    schema = new StructType().add("value", BinaryType),
//    flat = true,
//    serializer = Seq(
//      EncodeUsingSerializer(
//        BoundReference(0, CypherAnyOrNullType, nullable = true), kryo = true)),
//    deserializer =
//      DecodeUsingSerializer[T](
//        ReadColumn(0),
//        classTag[T],
//        kryo = true),
//    clsTag = classTag[T]
//  )


  //    ExpressionEncoder[T](
//      schema = new StructType().add("cypher", CypherAnyOrNullType),
//      flat = true,
//      serializer = Seq(
//        EncodeUsingSerializer(
//          BoundReference(0, CypherAnyOrNullType, nullable = true), kryo = true)),
//      deserializer =
//        DecodeUsingSerializer[T](
//          CypherCast(GetColumnByOrdinal(0, BinaryType)),
//          classTag[T],
//          kryo = true),
//      clsTag = classTag[T]
//    )

}

object SparkGraphImpl extends CypherImplicits {

  val session: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()

  val sc = session.sqlContext


//  implicit def AnyEncoder extends Encoder[Any] {
//    override def schema: StructType = ???
//    override def clsTag: ClassManifest[Any] = ???
//  }

  def apply(): SparkGraphImpl = {


    val nodes = sc.createDataset(Seq(CypherNode("prop" -> CypherString("value")), CypherNode("Label"), CypherNode("Label1", "Label2"), CypherNode("name" -> CypherString("foo"))))
    val relationships = sc.createDataset(Seq(CypherRelationship(0, 1, "KNOWS")))

    SparkGraphImpl(nodes, relationships, sc)
  }
}


