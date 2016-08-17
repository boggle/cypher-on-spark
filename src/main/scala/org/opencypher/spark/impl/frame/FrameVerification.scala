package org.opencypher.spark.impl.frame

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.impl.error.{StdError, StdErrorInfo}
import org.opencypher.spark.impl.util.Verification

object FrameVerification {

  abstract class Error(detail: String)(implicit private val info: StdErrorInfo)
    extends Verification.Error(detail) {
    self: Product with Serializable =>
  }

  abstract class TypeError(msg: String)(implicit info: StdErrorInfo) extends Error(msg) {
    self: Product with Serializable =>
  }

  final case class IsNoSuperTypeOf(superType: CypherType, baseType: CypherType)(implicit info: StdErrorInfo)
    extends TypeError(
      s"Supertype expected, but $superType is not a supertype of $baseType"
    )(info)

  final case class IsNoSubTypeOf(subType: CypherType, baseType: CypherType)(implicit info: StdErrorInfo)
    extends TypeError(
      s"Subtype expected, but $subType is not a subtype of $baseType"
    )(info)

  final case class UnInhabitedMeetType(lhsType: CypherType, rhsType: CypherType)(implicit info: StdErrorInfo)
    extends TypeError(s"There is no value of both type $lhsType and $rhsType")(info)

  final case class FrameSignatureMismatch(msg: String)(implicit info: StdErrorInfo)
    extends Error(msg)(info)

  final case class SlotNotEmbeddable(key: Symbol)(implicit info: StdErrorInfo)
    extends Error(s"Cannot use slot $key that relies on a non-embedded representation")(info)
}
