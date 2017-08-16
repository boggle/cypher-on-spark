package org.opencypher.spark.api.io

import java.io.File
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.opencypher.spark.api.record.{NodeScan, RelationshipScan}
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkCypherRecords, SparkGraphSpace}

/**
  * Loads a graph stored in indexed CSV format from HDFS or the local file system
  * The CSV files must be stored following this schema:
  * # Nodes
  *   - all files describing nodes are stored in a subfolder called "nodes"
  *   - create one file for each possible label combination that exists in the data, i.e. there must not be overlapping
  *     entities in different files (e.g. all nodes with labels :Person:Employee in a single file and all nodes that
  *     have label :Person exclusively in another file)
  *   - for every node csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the node schema file see [[CsvNodeSchema]]
  * # Relationships
  *   - all files describing relationships are stored in a subfolder called "relationships"
  *   - create one csv file per relationship type
  *   - for every relationship csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the relationship schema file see [[CsvRelSchema]]

  *
  * @param location Location of the top level folder containing the node and relationship files
  * @param graphSpace
  * @param sc
  */
class CsvGraphLoader(location: String)(implicit graphSpace: SparkGraphSpace, sc: SparkSession) {
  private val fs: FileSystem = FileSystem.get(new URI(location), sc.sparkContext.hadoopConfiguration)

  def load: SparkCypherGraph = {
    val nodeScans = loadNodes
    val relScans = loadRels
    SparkCypherGraph.create(nodeScans.head, nodeScans.tail ++ relScans: _*)
  }

  private def loadNodes: Array[NodeScan] = {
    val nodeLocation = s"$location${File.separator}nodes"
    val csvFiles = listCsvFiles(nodeLocation)

    csvFiles.map(e => {
      val schema = readSchema(e)(CsvNodeSchema(_))

      val records = SparkCypherRecords.create(
        sc.read
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
          .schema(schema.toStructType)
          .csv(e.toUri.toString)
      )

      NodeScan.on("n" -> schema.idField.name)(builder => {
        val withImpliedLabels = schema.implicitLabels.foldLeft(builder.build)(_ withImpliedLabel _)
        val withOptionalLabels = schema.optionalLabels.foldLeft(withImpliedLabels)((a, b) => {
          a.withOptionalLabel(b.name -> b.name)
        })
        schema.propertyFields.foldLeft(withOptionalLabels)((builder, field) => {
          builder.withPropertyKey(field.name -> field.name)
        })
      }).from(records)
    })
  }

  private def loadRels: Array[RelationshipScan] = {
    val relLocation = s"$location${File.separator}relationships"
    val csvFiles = listCsvFiles(relLocation)

    csvFiles.map(e => {
      val schema = readSchema(e)(CsvRelSchema(_))

      val records = SparkCypherRecords.create(
        sc.read
          .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
          .schema(schema.toStructType)
          .csv(e.toUri.toString)
      )

      RelationshipScan.on("r" -> schema.idField.name)(builder => {
        val baseBuilder = builder
          .from(schema.startIdField.name)
          .to(schema.endIdField.name)
          .relType(schema.relType)
            .build

        schema.propertyFields.foldLeft(baseBuilder)((builder, field) => {
          builder.withPropertyKey(field.name -> field.name)
        })
      }).from(records)
    })
  }

  private def listCsvFiles(directory: String): Array[Path] = {
    fs.listStatus(new Path(directory))
      .filterNot(_.getPath.toString.endsWith(".SCHEMA"))
      .map(_.getPath)
  }

  private def readSchema[T <: CsvSchema](path: Path)(parser: String => T): T = {
    val schemaPath = path.suffix(".SCHEMA")
    val stream = fs.open(schemaPath)
    def readLines = Stream.cons(stream.readLine(), Stream.continually(stream.readLine))
    parser(readLines.takeWhile(_ != null).mkString)
  }
}
