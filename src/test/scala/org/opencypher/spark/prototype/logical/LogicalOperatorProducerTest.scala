package org.opencypher.spark.prototype.logical

import org.opencypher.spark.prototype._
import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.global.{ConstantRef, PropertyKeyRef}
import org.opencypher.spark.prototype.ir.pattern.{DirectedRelationship, EveryNode, Pattern}
import org.scalatest.matchers.{MatchResult, Matcher}

class LogicalOperatorProducerTest extends IrTestSuite {

  test("convert match block") {
    val pattern = Pattern.empty[Expr]
      .withEntity('a, EveryNode())
      .withEntity('b, EveryNode())
      .withConnection('r, DirectedRelationship('a, 'b))

    val block = matchBlock(pattern)

    val scan = NodeScan('a, EveryNode())(emptySqm.withField('a))
    plan(irFor(block)) should equalWithoutResult(
      ExpandSource('a, 'r, 'b, scan)(scan.solved.withFields('r, 'b))
    )
  }

  val emptySqm = SolvedQueryModel.empty[Expr]

  test("convert project block") {
    val fields = ProjectedFields[Expr](Map(toField('a) -> Property('n, PropertyKeyRef(0))))
    val block = project(fields)

    plan(irWithLeaf(block)) should equalWithoutResult(
      Project(Property('n, PropertyKeyRef(0)), leafPlan)(emptySqm.withFields('n, 'a))
    )
  }

  test("plan query") {
    val ir = "MATCH (a:Administrator)-[r]->(g:Group) WHERE g.name = 'Group-1' RETURN a.name".ir

    val globals = ir.model.globals

    plan(ir) should equal(
      Select(Seq(Var("a.name") -> "a.name"),
        Project(Property(Var("a"), globals.propertyKey("name")),
          Filter(Equals(Property(Var("g"), globals.propertyKey("name")), Const(ConstantRef(0))),
            Project(Property(Var("g"), globals.propertyKey("name")),
              Filter(HasLabel(Var("g"), globals.label("Group")),
                Filter(HasLabel(Var("a"), globals.label("Administrator")),
                  ExpandSource(Var("a"), Var("r"), Var("g"),
                    NodeScan(Var("a"), EveryNode())(emptySqm)
                  )(emptySqm)
                )(emptySqm)
              )(emptySqm)
            )(emptySqm)
          )(emptySqm)
        )(emptySqm)
      )(emptySqm)
    )
  }

  private val producer = new LogicalOperatorProducer

  private def plan(ir: CypherQuery[Expr]): LogicalOperator =
    producer.plan(ir)

  case class equalWithoutResult(plan: LogicalOperator) extends Matcher[LogicalOperator] {
    override def apply(left: LogicalOperator): MatchResult = {
      left match {
        case Select(_, in) =>
          val matches = in == plan && in.solved == plan.solved
          MatchResult(matches, s"$in did not equal $plan", s"$in was not supposed to equal $plan")
        case _ => MatchResult(matches = false, "Expected a Select plan on top", "Expected a Select plan on top")
      }
    }
  }
}
