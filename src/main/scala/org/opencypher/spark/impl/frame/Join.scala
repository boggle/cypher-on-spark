package org.opencypher.spark.impl.frame

import org.apache.spark.sql.{Dataset, Row, functions}
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.impl._
import org.opencypher.spark.impl.util.Verification

object Join {

  import org.opencypher.spark.impl.util.Verification._

  def apply(lhs: StdCypherFrame[Row], rhs: StdCypherFrame[Row])
           (lhsKey: Symbol, rhsKey: Symbol): StdCypherFrame[Row] = {
    val lhsField = lhs.signature.field(lhsKey)
    val lhsType = lhsField.cypherType
    val lhsSlot = lhs.signature.slot(lhsField)
    val rhsField = rhs.signature.field(rhsKey)
    val rhsType = rhsField.cypherType
    val rhsSlot = rhs.signature.slot(rhsField)

    requireNonEmptyJoin(lhsType, rhsType)
    requireEmbeddedRepresentation(lhsKey, lhsSlot)
    requireEmbeddedRepresentation(rhsKey, rhsSlot)

    // TODO: Should the join slots have the same representation?
    Join(lhs, rhs)(lhsField, rhsField)
  }

  private final case class requireNonEmptyJoin(lhsType: CypherType, rhsType: CypherType) extends Verification {
    ifNot((lhsType meet rhsType).isInhabited.maybeTrue) failWith FrameVerification.UnInhabitedMeetType(lhsType, rhsType)
  }

  private final case class requireEmbeddedRepresentation(lhsKey: Symbol, lhsSlot: StdSlot) extends Verification {
    ifNot(lhsSlot.representation.isEmbedded) failWith FrameVerification.SlotNotEmbeddable(lhsKey)
  }

  private final case class Join(lhs: StdCypherFrame[Row], rhs: StdCypherFrame[Row])
                               (lhsKey: StdField, rhsKey: StdField)
    extends StdCypherFrame[Row](lhs.signature ++ rhs.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Row] = {
      val lhsIn = lhs.run
      val rhsIn = rhs.run

      val lhsSlot = signature.slot(lhsKey)
      val rhsSlot = signature.slot(rhsKey)

      val joinExpr = functions.expr(s"${lhsSlot.sym.name} = ${rhsSlot.sym.name}")

      lhsIn.join(rhsIn, joinExpr)
    }
  }
}
