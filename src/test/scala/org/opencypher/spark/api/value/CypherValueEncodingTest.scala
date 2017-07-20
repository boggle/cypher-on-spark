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
package org.opencypher.spark.api.value

import org.opencypher.spark.api.value.CypherValue._
import org.opencypher.spark.{BaseTestSuite, SparkTestSession}

class CypherValueEncodingTest extends BaseTestSuite with SparkTestSession.Fixture {

  import CypherTestValues._

  test("PATH encoding") {
    val values = PATH_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("RELATIONSHIP encoding") {
    val values = RELATIONSHIP_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("NODE encoding") {
    val values = NODE_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("MAP encoding") {
    val values = MAP_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("LIST encoding") {
    val values = LIST_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("STRING encoding") {
    val values = STRING_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("BOOLEAN encoding") {
    val values = BOOLEAN_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("INTEGER encoding") {
    val values = INTEGER_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("FLOAT encoding") {
    val values = FLOAT_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("NUMBER encoding") {
    val values = NUMBER_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }

  test("ANY encoding") {
    val values = ANY_valueGroups.flatten
    val ds = session.createDataset[CypherValue](values)(Encoders.cypherValueEncoder)

    ds.collect().toSeq should equal(values)
  }
}
