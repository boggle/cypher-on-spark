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
package org.opencypher.caps.impl.spark

import org.opencypher.caps.api.expr.{Expr, Var}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSSession}
import org.opencypher.caps.api.value.CypherValue

object operations {

  implicit final class RichCAPSGraph(val graph: CAPSGraph) extends AnyVal {

    def filter(subject: CAPSRecords, expr: Expr, parameters: Map[String, CypherValue] = Map.empty)
              (implicit caps: CAPSSession)
    : CAPSRecords =
      caps.filter(graph, subject, expr, parameters)

    def select(subject: CAPSRecords, fields: IndexedSeq[Var], parameters: Map[String, CypherValue] = Map.empty)
              (implicit caps: CAPSSession): CAPSRecords =
      caps.select(graph, subject, fields, parameters)

    def project(subject: CAPSRecords, expr: Expr, parameters: Map[String, CypherValue] = Map.empty)
               (implicit caps: CAPSSession): CAPSRecords =
      caps.project(graph, subject, expr, parameters)

    def alias(subject: CAPSRecords, alias: (Expr, Var), parameters: Map[String, CypherValue] = Map.empty)
             (implicit caps: CAPSSession): CAPSRecords =
      caps.alias(graph, subject, alias, parameters)
  }
}
