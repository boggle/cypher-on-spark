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
package org.opencypher.spark

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.opencypher.spark_legacy.benchmark.Configuration.{Logging, Neo4jAddress, Neo4jPassword, Neo4jUser}
import org.opencypher.spark_legacy.{CypherKryoRegistrar, PropertyGraphFactory}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

object SparkTestSession {

  lazy val default = Factory.create

  trait Fixture extends BeforeAndAfterEach {
    self: FunSuite =>

    implicit val session = SparkTestSession.default
  }

  object Factory {
    def create = {
      val conf = new SparkConf(true)
      conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)
      conf.set("spark.neo4j.bolt.password", Neo4jPassword.get())
      conf.set("spark.neo4j.bolt.user", Neo4jUser.get())
      conf.set("spark.neo4j.bolt.url", Neo4jAddress.get())

      //
      // This may or may not help - depending on the query
      // conf.set("spark.kryo.referenceTracking","false")

      //
      // Enable to see if we cover enough
      // conf.set("spark.kryo.registrationRequired", "true")

      //
      // If this is slow, you might be hitting: http://bugs.java.com/view_bug.do?bug_id=8077102
      //
      val session = SparkSession
        .builder()
        .config(conf)
        .master("local[*]")
        .appName(s"cypher-for-apache-spark-tests-${UUID.randomUUID()}")
        .getOrCreate()

      session.sparkContext.setLogLevel(Logging.get())
      session
    }
  }
}
