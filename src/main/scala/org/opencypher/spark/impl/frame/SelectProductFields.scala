package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl._

object SelectProductFields {

  def apply(input: StdCypherFrame[Product])(fields: Symbol*): StdCypherFrame[Product] = {
    val (slotMapping, newSignature) = input.signature.selectFields(fields: _*)
    SelectProductFields(input)(newSignature, slotMapping)
  }

  private final case class SelectProductFields(input: StdCypherFrame[Product])(sig: StdFrameSignature, slots: Seq[StdSlot])
    extends ProductFrame(sig) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val out = input.run.map(selectFields(slots))(context.productEncoder(sig.slots))
      out
    }
  }

  private final case class selectFields(slots: Seq[StdSlot]) extends (Product => Product) {

    import org.opencypher.spark.impl.util._

    def apply(product: Product): Product = {
      val builder = Vector.newBuilder[Any]
      builder.sizeHint(slots.size)
      slots.foreach { slot => builder += product.get(slot.ordinal) }
      val newValue = builder.result()
      newValue.asProduct
    }
  }
}
