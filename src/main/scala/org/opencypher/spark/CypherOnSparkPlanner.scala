package org.opencypher.spark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}

class CypherOnSparkPlanner extends CypherImplicits {

  def plan(cypherPart: CypherPart, graph: SparkGraph) = {
//    cypherPart match {
//      case m: MatchingPart => // MATCH (n) WHERE n.name = 'foo' RETURN n.name
        val v = graph.nodes.filter { node: CypherNode =>
          node.properties.v.get("name").contains(CypherString("foo"))
        }

    graph.nodes.show()
    v.show()
        v.printSchema()

    val b = v.map { n: CypherNode =>
      n.properties.v("name")
    }

    b.show()
    b.printSchema()

    b.withColumnRenamed("value", "n.name")

    b

//        val b: DataFrame = v.map(n => Row(n))
//        b.printSchema()


//        b.select(Column)

//        v.select(Columns)
//    }

  }

  def plan2(cypherPart: CypherPart, graph: SparkGraph) = {
    // MATCH (n) WITH { foo: n } AS m RETURN m.foo.prop AS c
    val v = graph.nodes.map { n: CypherNode =>
      CypherMap(Map("foo" -> n))
    }

    v.map { map: CypherMap =>
      map.v("foo").asInstanceOf[CypherNode].properties.v.get("prop").orNull
    }.withColumnRenamed("value", "c")

//     v
  }

  def plan3(cypherPart: CypherPart, graph: SparkGraph) = {
    // UNWIND [1, '3', 2] AS i RETURN i ORDER BY i ASC
    val v: Dataset[CypherValue] = graph.sqlContext.createDataset(Seq(CypherInteger(1), CypherString("3"), CypherInteger(2)))
    v.orderBy("value")
    v.rdd.a
  }
}
