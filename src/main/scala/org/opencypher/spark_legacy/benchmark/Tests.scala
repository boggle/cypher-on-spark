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

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.value.{CypherNode, CypherRelationship}

object Tests {

  def main(args: Array[String]): Unit = {
    writeGraph(RunBenchmark.sparkSession)
//    readGraph(sparkSession)
//    write(sparkSession)
//    read(sparkSession)
  }

  def read(sparkSession: SparkSession) = {
    val intRDD = sparkSession.sparkContext.objectFile[Int](INT_FILE_PATH)

    val sum = intRDD.reduce {
      case (i, j) => i + j
    }

    println(s"sum: $sum")

    import sparkSession.implicits._

    val dataset = sparkSession.createDataset(intRDD).cache()
  }

  val INT_FILE_PATH = "/Users/mats/gitRoots/sparkfork/CypherOnSpark/spark-warehouse/rdds/ints2"
  val NODES_PATH = "/Users/mats/gitRoots/sparkfork/CypherOnSpark/spark-warehouse/rdds/nodes"
  val RELS_PATH = "/Users/mats/gitRoots/sparkfork/CypherOnSpark/spark-warehouse/rdds/rels"

  def readGraph(sparkSession: SparkSession) = {
    val start = System.currentTimeMillis()
    val nodeRDD = sparkSession.sparkContext.objectFile[CypherNode](NODES_PATH)
    val relsRDD = sparkSession.sparkContext.objectFile[CypherRelationship](RELS_PATH)
    val time = System.currentTimeMillis() - start

    println(s"Read the data from disk in $time ms")
    printSums(nodeRDD, relsRDD)
  }

  def printSums(nodes: RDD[CypherNode], rels: RDD[CypherRelationship]) = {
    val nodeSum = nodes.count() //nodes.map(_.hashCode()).sum()
    val relSum = rels.count() //rels.map(_.hashCode()).sum()

    println(s"nodesum: $nodeSum")
    println(s"relsum: $relSum")
  }

  def writeGraph(sparkSession: SparkSession) = {
    val start = System.currentTimeMillis()
    val (nodes, rels) = RunBenchmark.loadRDDs()

    printSums(nodes, rels)
    val time = System.currentTimeMillis() - start
    println(s"Imported the data from Neo4j in $time ms")

    nodes.saveAsObjectFile(NODES_PATH)
    rels.saveAsObjectFile(RELS_PATH)
  }

  def write(sparkSession: SparkSession) = {
    val ints = 0 to 1000

    val intRDD = sparkSession.sparkContext.parallelize(ints)

    intRDD.saveAsObjectFile(INT_FILE_PATH)
  }

}
