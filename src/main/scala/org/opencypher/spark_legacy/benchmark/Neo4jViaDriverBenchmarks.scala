/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark_legacy.benchmark

import org.neo4j.driver.v1.{Driver, Session, Value}
import org.opencypher.spark_legacy.impl.SupportedQuery

object Neo4jViaDriverBenchmarks extends SupportedQueryBenchmarks[Neo4jViaDriverGraph] {

  def apply(query: SupportedQuery): Benchmark[Neo4jViaDriverGraph] =
    new Neo4jViaDriverBenchmark(query.toString)
}

class Neo4jViaDriverBenchmark(query: String) extends Benchmark[Neo4jViaDriverGraph] {

  override def name: String = "Neo4j     "

  override def init(graph: Neo4jViaDriverGraph): Unit = {
    // TODO: Find better way to flush plan cache
    graph.withSession(session => session.run(s"CREATE CONSTRAINT ON (n:Foo) ASSERT n.baz IS UNIQUE"))
    graph.withSession(session => session.run(s"DROP CONSTRAINT ON (n:Foo) ASSERT n.baz IS UNIQUE"))
  }

  override def plan(graph: Neo4jViaDriverGraph): String =
    graph.withSession(session => session.run(s"EXPLAIN $query").consume().plan().toString)

  override def run(graph: Neo4jViaDriverGraph): Outcome = {
    graph.withSession { session =>
      val intType = session.typeSystem().INTEGER()
      var count = 0
      var checksum = 0
      val result = session.run(query)

      while (result.hasNext) {
        val record = result.next()
        count += 1
        (0 until record.size).foreach { i =>
          val value = record.get(i)
          val increment = if (value.hasType(intType)) value.asLong() else if (value.isNull) -1L else value.asEntity().id()
          checksum ^= increment.hashCode()
        }
      }
      val summary = result.consume()

      new Outcome {
        override val computeCount: Long = count
        override val computeChecksum: Int = checksum
        override def usedCachedPlan: Boolean = true
      }
    }
  }

  def numNodes(graph: Neo4jViaDriverGraph): Long =
    graph.withSession(_.run("MATCH (n) RETURN count(n)").single().get(0).asLong())

  def numRelationships(graph: Neo4jViaDriverGraph): Long =
    graph.withSession(_.run("MATCH ()-[r]->() RETURN count(r)").single().get(0).asLong())
}

class Neo4jViaDriverGraph(driver: Driver) {
  def withSession[T](f: Session => T): T = {
    val session = driver.session()
    try {
      f(session)
    } finally {
      session.close()
    }
  }
}

