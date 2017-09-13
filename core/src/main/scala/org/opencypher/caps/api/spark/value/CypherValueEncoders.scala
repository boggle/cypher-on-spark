package org.opencypher.caps.api.spark.value

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.opencypher.caps.api.value.{CypherNode, CypherRelationship, CypherValue}

object CypherValueEncoders extends CypherValueEncoders

trait CypherValueEncoders extends CypherValueLowPriorityEncoders {
  implicit def cypherNodeEncoder: ExpressionEncoder[CypherNode] = kryo[CypherNode]
  implicit def cypherRelationshipEncoder: ExpressionEncoder[CypherRelationship] = kryo[CypherRelationship]
}

trait CypherValueLowPriorityEncoders {
  implicit def asExpressionEncoder[T](v: Encoder[T]): ExpressionEncoder[T] = v.asInstanceOf[ExpressionEncoder[T]]
  implicit def cypherValueEncoder: ExpressionEncoder[CypherValue] = kryo[CypherValue]
  implicit def cypherRecordEncoder: ExpressionEncoder[Map[String, CypherValue]] = kryo[Map[String, CypherValue]]
}
