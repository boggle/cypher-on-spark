package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark._
import org.opencypher.spark.api._
import org.opencypher.spark.impl._
import org.opencypher.spark.impl.frame.StdFrameTestSuite.FrameTestResult
import org.opencypher.spark.impl.util.SlotSymbolGenerator

object StdFrameTestSuite {
  final case class FrameTestResult[Out](dataframe: Dataset[Out], signature: StdFrameSignature) {
    def toList = dataframe.collect().toList
    def toSet = dataframe.collect().toSet
  }
}

abstract class StdFrameTestSuite extends StdTestSuite with TestSession.Fixture {

  trait GraphTest {
    val graph = factory.graph

    implicit val planningContext =
      new PlanningContext(new SlotSymbolGenerator, graph.nodes, graph.relationships)

    val frames = new FrameProducer

    implicit val runtimeContext = new StdRuntimeContext(session)
  }

  def add(nodeData: NodeData) = factory.add(nodeData)
  def add(relationshipData: RelationshipData) = factory.add(relationshipData)

  implicit final class RichTestFrame[Out](val frame: StdCypherFrame[Out]) extends AnyRef {

    def testResult(implicit context: StdRuntimeContext) = {
      val out = frame.run(context)
      out.columns should equal(frame.slots.map(_.sym.name))
      FrameTestResult(out, frame.signature)
    }
  }

  implicit final class RichFrameSignature(val sig: CypherFrameSignature) extends AnyRef {

    def shouldHaveFields(expected: (Symbol, CypherType)*): Unit = {
      sig.fields.map { field => field.sym -> field.cypherType } should equal(expected)
    }

    def shouldHaveFieldSlots(expected: (Symbol, Representation)*): Unit = {
      sig.fields.map { f => f.sym -> sig.slot(f.sym).representation }.toSet should equal(expected.toSet)
    }
  }
}
