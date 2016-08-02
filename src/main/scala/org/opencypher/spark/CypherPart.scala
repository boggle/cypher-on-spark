package org.opencypher.spark

trait CypherPart {

  def orderBy: OrderBy

  def projection: With

  def aggregation: Aggregation

  def tail: Option[CypherPart]

}

trait Pattern {
  def kind: PatternKind
}

case class StandardPattern(kind: PatternKind) extends Pattern {

}


trait Horizon

sealed trait PatternKind

case object RegularPattern extends PatternKind

case object OptionalPattern extends PatternKind

trait Aggregation

trait With

trait OrderBy

/*
 * Projection is a clause that can remove or overwrite variables in the context
 * WITH, ORDER BY, aggregation
 *
 * Other clauses may only extend the context with new variables
 */

// MATCH (a)-[r1]->(b)-[r2]->(c)
// MATCH (a)-[r3]->(d)
// WHERE a.name = 'foo'
//

trait QueryGraph {
//  def nodes: Set[Node]

//  def relationships: Set[Relationship]

//  def predicates: Set[Predicate]
}

trait MatchingPart extends CypherPart {

  def queryGraph: QueryGraph
  def optional: Boolean

  override def orderBy: OrderBy = ???

  override def projection: With = ???

  override def aggregation: Aggregation = ???

  override def tail: Option[CypherPart] = ???
}

// UNWIND x IN list

//trait UnwindPart extends CypherPart {
//
//  def list: ListExpression
//
//  def variable: Variable
//
//  override def orderBy: OrderBy = OrderBy.none
//
//  override def projection: With = ???
//
//  override def aggregation: Aggregation = Aggregation.none
//
//  override def tail: Option[CypherPart] = ???
//}
