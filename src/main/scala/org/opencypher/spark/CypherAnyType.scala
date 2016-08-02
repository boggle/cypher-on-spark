package org.opencypher.spark

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.serializer.{JavaSerializer, JavaSerializerInstance, KryoSerializer, KryoSerializerInstance}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, NonSQLExpression, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType, ObjectType}

import scala.reflect.ClassTag

object CypherAnyType extends DataType with Serializable {

  override def asNullable = CypherAnyOrNullType

  override def defaultConcreteType: DataType = this

  override def acceptsType(other: DataType): Boolean = other match {
    case CypherAnyType => true
    case _ => false
  }

  override def simpleString: String = "ANY"

  override def defaultSize: Int = 256
}

object CypherAnyOrNullType extends DataType with Serializable {

  override def asNullable = this

  override def defaultConcreteType: DataType =
    throw new UnsupportedOperationException("this may be null?")

  override def acceptsType(other: DataType): Boolean = other match {
    case CypherAnyOrNullType => true
    case CypherAnyType => true
    case _ => false
  }

  override def simpleString: String = "ANY?"

  override def defaultSize: Int = 256
}

case class CypherCast(child: Expression) extends UnaryExpression with NullIntolerant {

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    val input = child.genCode(ctx)
//    val code = s"""
//       |final Object ${ev.value} = ${input.value};
//     """.stripMargin
    input
  }

  override val dataType: DataType = CypherAnyOrNullType
}
case class ReadColumn(ordinal: Int) extends LeafExpression with NullIntolerant {

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    val code = s"""
       |final boolean ${ev.isNull} = ${ctx.INPUT_ROW} == null ? true : ${ctx.INPUT_ROW}.isNullAt($ordinal);
       |final byte[] ${ev.value} = ${ctx.INPUT_ROW} == null ? null : ${ctx.INPUT_ROW}.getBinary($ordinal);
     """.stripMargin
    ev.copy(code = code)
  }

  override val dataType: DataType = BinaryType

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = input.getBinary(ordinal)
}

case class ByteArrayCast(child: Expression) extends UnaryExpression with NullIntolerant {

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    val input = child.genCode(ctx)
    val code = s"""
                  |final byte[] ${ev.value} = (byte[]) ${input.value};
     """.stripMargin
    ev.copy(code = code, isNull = input.isNull)
  }

  override val dataType: DataType = BinaryType
}
