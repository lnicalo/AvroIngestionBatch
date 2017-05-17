/**
  * Created by lnicalo on 21/04/17.
  */
package com.lnicalo.AvroIngestion

import java.io.{File, FileInputStream}

import com.samelamin.spark.bigquery._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

object ExportToBQ extends App {
  private def loadConfig(filename: String) = {
    val input = new FileInputStream(new File(filename))
    val yaml = new Yaml(new Constructor(classOf[ExportToBQConfig]))
    yaml.load(input).asInstanceOf[ExportToBQConfig]
  }

  // Set proxy configuration

  val confFilePath = args.headOption.getOrElse("./data/confBQ.yaml")
  val config = loadConfig(confFilePath)

  if(config.proxyHost != "None") {
    val systemProperties = System.getProperties()
    systemProperties.setProperty("https.proxyHost", config.proxyHost)
    systemProperties.setProperty("https.proxyPort", config.proxyPort)
  }

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("ParquetToBQIngestion")
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .getOrCreate()
  spark.conf.set("spark.sql.streaming.checkpointLocation", config.checkpointLocation)
  import spark.implicits._
  val schema = new StructType()
    .add("db_catalog_key", "string")
    .add("signal_name", "string")
    .add("event_time", "string")
    .add("value", "string")

  // Set up GCP credentials
  spark.sqlContext.setGcpJsonKeyFile(config.jsonKeyFile)

  // Set up BigQuery project and bucket
  spark.sqlContext.setBigQueryProjectId(config.bigQueryProjectId)
  spark.sqlContext.setBigQueryGcsBucket(config.bigQueryGcsBucket)

  // Set up BigQuery dataset location
  spark.sqlContext.setBigQueryDatasetLocation(config.bigQueryDatasetLocation)

  spark.sparkContext.hadoopConfiguration.set("fs.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
  spark.sparkContext.hadoopConfiguration.set("fs.gs.project.id", config.bigQueryProjectId)

  // Read stream of parquet files
  val df = spark
    .readStream
    .schema(schema)
    .parquet(config.inputDir)

  // Write stream on BigQuery
  val stream = df
    .withColumn(config.timeColName, Symbol(config.timeColName).cast(DoubleType))
    .writeStream
    .option("checkpointLocation", config.checkpointLocation)
    .option("tableReferenceSink", config.tableReferenceSink)
    .format("com.samelamin.spark.bigquery")
    .outputMode("append")
    .start()
    .awaitTermination()
    //.option("header", "true")
    //.format("csv")
    //.outputMode("append")
    //.start("./data/bq/")
    //.awaitTermination()
}
