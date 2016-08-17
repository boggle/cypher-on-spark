package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl._
import org.opencypher.spark.impl.util.Verification

object UnionAll {

  def apply(lhs: StdCypherFrame[Product], rhs: StdCypherFrame[Product]): StdCypherFrame[Product] = {
    val lhsFields = lhs.signature.fields
    val rhsFields = rhs.signature.fields
    requireMatchingFrameFields(lhsFields, rhsFields)

    UnionAll(lhs, rhs)
  }

  private final case class requireMatchingFrameFields(lhsFields: Seq[StdField], rhsFields: Seq[StdField])
    extends Verification {
    ifNot(lhsFields.equals(rhsFields)) failWith FrameVerification.FrameSignatureMismatch(
      s"""Fields of lhs and rhs of UNION must be equal
          |$lhsFields
          |$rhsFields
      """.stripMargin
    )
  }

  private final case class UnionAll(lhs: StdCypherFrame[Product], rhs: StdCypherFrame[Product])
    extends StdCypherFrame[Product](lhs.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val lhsIn = lhs.run
      val rhsIn = rhs.run

      val union = lhsIn.union(rhsIn)

      union
    }
  }
}
