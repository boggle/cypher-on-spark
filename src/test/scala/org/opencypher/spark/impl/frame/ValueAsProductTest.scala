package org.opencypher.spark.impl.frame

import org.opencypher.spark.api.BinaryRepresentation
import org.opencypher.spark.api.types.CTNode

class ValueAsProductTest extends StdFrameTestSuite {

  test("ValueAsProduct converts DataSet[CypherNode]") {
    val a = add(newNode.withLabels("A").withProperties("name" -> "Zippie"))
    val b = add(newNode.withLabels("B").withProperties("name" -> "Yggie"))

    new GraphTest {
      import frames._

      val result = allNodes('n).asProduct.testResult

      result.signature shouldHaveFields ('n -> CTNode)
      result.signature shouldHaveFieldSlots ('n -> BinaryRepresentation)
      result.toSet should equal(Set(a, b).map(Tuple1(_)))
    }
  }
}
