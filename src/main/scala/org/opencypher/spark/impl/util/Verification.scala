package org.opencypher.spark.impl.util

import org.opencypher.spark.impl.error.{StdError, StdErrorInfo}

object Verification {

  final class Verifier[T <: Verification.Error] private[impl](cond: => Boolean) {
    def failWith(error: => T) = if (!cond) throw error
  }

  abstract class Error(override val detail: String)(implicit private val info: StdErrorInfo)
    extends StdError(detail) {
    self: Product with Serializable =>
  }

}

trait Verification extends StdErrorInfo.Implicits {

  import Verification._

  def ifNot[T <: Error](cond: => Boolean): Verifier[T] =
    new Verifier(cond)
}






