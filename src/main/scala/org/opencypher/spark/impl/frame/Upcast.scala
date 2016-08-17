package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.impl._
import org.opencypher.spark.impl.util.Verification

import scala.language.postfixOps

object Upcast {

  def apply[Out](input: StdCypherFrame[Out])(fieldSym: Symbol)(widen: CypherType => CypherType)
                (implicit context: PlanningContext): StdCypherFrame[Out] = {

    val field = input.signature.field(fieldSym)
    val oldType = field.cypherType
    val newType = widen(oldType)

    requireIsSuperTypeOf(newType, oldType)

    val (_, sig) = input.signature.upcastField(field.sym, newType)
    CypherUpcast[Out](input)(sig)
  }

  private final case class requireIsSuperTypeOf(newType: CypherType, oldType: CypherType) extends Verification {
    ifNot(newType `superTypeOf` oldType isTrue) failWith FrameVerification.IsNoSuperTypeOf(newType, oldType)
  }

  private final case class CypherUpcast[Out](input: StdCypherFrame[Out])(sig: StdFrameSignature)
    extends StdCypherFrame[Out](sig) {

    override def execute(implicit context: RuntimeContext): Dataset[Out] = {
      val out = input.run
      out
    }
  }
}
