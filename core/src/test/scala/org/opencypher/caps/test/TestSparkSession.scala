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
/**
  * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.opencypher.caps.test

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.opencypher.caps.demo.Configuration.Logging
import org.opencypher.caps.demo.CypherKryoRegistrar

object TestSparkSession {

  lazy val instance: SparkSession = {
    val conf = new SparkConf(true)
    conf.set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    conf.set("spark.kryo.registrator", classOf[CypherKryoRegistrar].getCanonicalName)

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



