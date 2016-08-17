package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl.newvalue.CypherValue
import org.opencypher.spark.impl.{StdCypherFrame, StdField, StdRuntimeContext}

import scala.reflect._

object OrderBy {

  def apply(input: StdCypherFrame[Product])(key: Symbol): StdCypherFrame[Product] = {
    val keyField = input.signature.field(key)
    OrderBy(input)(keyField)
  }

  private final case class OrderBy(input: StdCypherFrame[Product])(key: StdField) extends StdCypherFrame[Product](input.signature) {

    val index = signature.slot(key).ordinal

    override protected def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      val in = input.run

      val sortedRdd = in.rdd.sortBy(OrderByColumn(index))(CypherValue.orderability, classTag[CypherValue])

      val out = context.session.createDataset(sortedRdd)(context.productEncoder(slots))
      out
    }
  }

  private final case class OrderByColumn(index: Int) extends (Product => CypherValue) {

    import org.opencypher.spark.impl.util._

    override def apply(product: Product): CypherValue = {
      product.getAs[CypherValue](index)
    }
  }

}
