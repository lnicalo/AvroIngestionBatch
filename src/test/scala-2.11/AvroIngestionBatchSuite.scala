package com.lnicalo.AvroIngestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import com.databricks.spark.avro._

import org.scalatest.{FunSuite, ShouldMatchers}

/**
  * Created by lnicalo on 12/04/17.
  */
class AvroIngestionBatchSuite extends FunSuite with ShouldMatchers {
  val inputDataDir = "./data/input/data/2016/03/12"
  val inputDbCatDir = "./data/input/db_catalog"

  val outputMapDir = "./data/output/map/"
  val outputDataDir = "./data/output/data/"
  val outputDbCatDir = "./data/output/db_catalog"

  test("data integrity") {
    val spark = SparkSession.builder
      .master("local")
      .appName("AvroIngestionBatchSuite - data integrity test")
      .getOrCreate()
    import spark.implicits._

    // Load input data
    val input_df = spark.read.avro(inputDataDir)
    val input = input_df.repartition(1)
      .select($"signal_name", $"db_catalog_key", $"event_time", $"value")
      .sort($"db_catalog_key", $"signal_name", $"event_time", $"value")
      .as[(String, String, String, String)].collect()

    // Load output data
    val output_df = spark.read.parquet(outputDataDir)
    val output = output_df.repartition(1)
      .select($"signal_name", $"db_catalog_key", $"event_time", $"value")
      .sort($"db_catalog_key", $"signal_name", $"event_time", $"value")
      .as[(String, String, String, String)].collect()

    // Test values
    input zip output foreach({case (row1, row2) => row1 should be (row2) })
  }

  test("map integrity") {
    val spark = SparkSession.builder
      .master("local")
      .appName("AvroIngestionBatchSuite - map integrity test")
      .getOrCreate()
    import spark.implicits._

    // Load input data
    val signal_map = spark.read.option("header", "true").csv(outputMapDir)
    val input = signal_map.repartition(1)
      .select($"signal_name", $"hash_signal_name")
      .sort($"signal_name").as[(String, String)].collect()

    // Load output data
    val output_df = spark.read.parquet(outputDataDir)
    val output = output_df.repartition(1)
      .select($"signal_name", $"hash_signal_name").distinct()
      .sort($"signal_name").as[(String, String)].collect()

    // Test values
    input zip output foreach({case (row1, row2) => row1 should be (row2) })
  }

  test("db catalog integrity") {
    val spark = SparkSession.builder
      .master("local")
      .appName("AvroIngestionBatchSuite - db catalog test")
      .getOrCreate()
    import spark.implicits._

    // Load input data
    val input_db_cat = spark.read.option("header", "true").csv(inputDbCatDir)
    val input_cols = input_db_cat.columns.map(str => col(str))
    val input = input_db_cat.select(input_cols:_*)
      .repartition(1)
      .sort($"id")
      .collect()

    // Load output data
    val output_df = spark.read.option("header", "true").csv(outputDbCatDir)
    val output = output_df.select(input_cols:_*)
      .repartition(1)
      .sort($"id")
      .collect()

    // Test values
    input zip output foreach({case (row1, row2) => row1 should be (row2) })
  }

}
