package org.opencypher.caps.api.graph

import org.opencypher.caps.api.value.CypherValue

/**
  * Common interface for objects that can execute a Cypher query
  *
  * @see [[CypherGraph]]
  * @see [[CypherResult]]
  * @see [[CypherSession.cypher()]]
  */
trait CypherQueryRunner {

  type Result <: CypherResult

  /**
    * Executes a Cypher query in this runner using an empty parameter map.
    *
    * @param query      the Cypher query to execute.
    * @return           the result of the query.
    * @see              [[CypherSession.cypher()]]
    */
  final def cypher(query: String): Result =
    cypher(query, Map.empty)

  def cypher(query: String, parameters: Map[String, CypherValue]): Result
}
