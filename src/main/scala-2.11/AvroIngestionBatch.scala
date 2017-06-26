/**
  * Created by LNICOLAS on 27/03/2017.
  */
package com.lnicalo.AvroIngestion

import com.databricks.spark.avro._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.hashing.{MurmurHash3 => MH3}
import org.apache.spark.sql.functions._
import java.io.{File, FileInputStream}

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.util.{Failure, Success, Try}

object AvroIngestionBatch extends App {
  // Loads yaml file with config
  private def loadConfig(filename: String) = {
    val input = new FileInputStream(new File(filename))
    val yaml = new Yaml(new Constructor(classOf[AvroIngestionBatchConfig]))
    yaml.load(input).asInstanceOf[AvroIngestionBatchConfig]
  }

  // Load config
  val confFilePath = args.headOption.getOrElse("./data/conf.yaml")
  val config = loadConfig(confFilePath)
  val max_total_size: Float = config.maxBatchSizeMillonOfMessages * 1000000

  // Create spark session
  val spark = SparkSession.builder
      .master("local[12]")
    .appName("AvroToParquetIngestion")
    .getOrCreate()

  import spark.implicits._
  // Settings to optimize performance on object storages
  spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
  spark.conf.set("spark.speculation", false)
  spark.conf.set("spark.hadoop.parquet.enable.summary-metadata", false)
  spark.conf.set("spark.sql.parquet.mergeSchema", false)
  spark.conf.set("spark.sql.parquet.filterPushdown", true)
  spark.conf.set("spark.sql.hive.metastorePartitioningPruning", true)

  // Used defined function to convert signals names into hash strings unsing Murmur hash
  val hash = udf((s: String) => MH3.stringHash(s, MH3.stringSeed))

  Logger.getLogger("org").setLevel(Level.WARN)

  // Logger
  val logger = Logger.getLogger("AvroIngestion")
  logger.info("Configuration file: " + confFilePath)
  logger.info("Input directory: " + config.inputDir)
  logger.info("Input db catalog: " + config.inputDbCat)
  logger.info("Output directory: " + config.outputDir)
  logger.info("Output db catalog: " + config.outputDbCat)
  logger.info("Tmp directory: " + config.tmpDir)
  logger.info("Max batch size bytes: " + config.maxBatchSizeMillonOfMessages)
  logger.info("Session column: " + config.sessionIDColNameDbCat)
  logger.info("Module column: " + config.moduleColName)

  while (true) {
    // Init timestamp
    val initBatchTimeStamp = System.currentTimeMillis
    // Batch size
    var total_size_batch: Float = 0


    // Read input db catalog
    logger.info("Reading input db cat")
    val input_db_cat = spark.read.option("header", "true").csv(config.inputDbCat).dropDuplicates(config.sessionIDColNameDbCat)

    // Read output db catalog or create an empty array of strings if it does not exist
    val processed_files: Array[String] = Try(
      spark.read.option("header", "true")
        .csv(config.outputDbCat)
        .select(config.sessionIDColNameDbCat).as[String].collect()).getOrElse(Array[String]())

    // Read avro file names to process
    logger.info("Reading input data")
    val dfs = input_db_cat
      .select(Symbol(config.sessionIDColNameDbCat), 'path, Symbol(config.filesizeColName).cast("Float"))
      .as[(String, String, Float)].collect()
      .sortBy({ case (_, _, size) => -size })
      // Stream ensures that we do not try to read all avro files
      .toStream
      .map({ case (id, filePath, size) => (id, config.inputDir + filePath, size) })
      .collect({ case (id, filePath, size) if !processed_files.contains(id) =>
        // logger.info("Checking: " + filePath + " Size: " + size + " Total load: " + total_size_batch)
        Try((id, filePath, size, spark.read.avro(filePath)))
      })
      .takeWhile({
        case Success((_, _, size, _)) =>
          // We process at least one file
          val o = total_size_batch < max_total_size
          // val left = Math.abs(total_size_batch - max_total_size)
          total_size_batch += size
          o
        case Failure(_) => true
      })
      .collect({ case Success((id, filePath, size, df)) =>
        logger.info("Processing: " + filePath + " Size: " + size + " Total load: " + total_size_batch)
        (id, df)
      })

    // Write parquet files with data
    if (dfs.nonEmpty) {
      // Union of dataframes with input files
      val df = dfs
        .map {x => x._2}
        .reduce { _.union(_) }
      val hashed_col = "hash_" + config.moduleColName
      val hashed_df = df
        .withColumn(hashed_col, hash(df(config.moduleColName)))
        // .cache()

      // Write output partitioned by signal name
      logger.info("Writing output data")
      hashed_df
        .repartition(col(hashed_col))
        .write
        .option("compression", "snappy")
        .mode(SaveMode.Append)
        .partitionBy(hashed_col)
        //.option("header", "true")
        //.csv(config.outputDir)
        .parquet(config.outputDir)

      // Try to read signal map. Create empty one if it does not exists.
      logger.info("Reading existing signal map")
      val existing_signal_map = Try(
        spark.read.option("header", "true").csv(config.outputSignalMap)
      ).getOrElse(Seq.empty[(String, String)].toDF(hashed_col, config.moduleColName))

      logger.info("Writing new signal map")
      hashed_df
        .select(hashed_col, config.moduleColName)
        .union(existing_signal_map)
        .distinct()
        .repartition(1)
        .write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv(config.tmpDir)

      spark.read.option("header", "true")
        .csv(config.tmpDir)
        .repartition(1)
        .sort(asc(config.moduleColName))
        .repartition(1)
        .write
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv(config.outputSignalMap)

      // Append files to db catalog
      logger.info("Writing new db catalog")
      val fileNames = dfs.map(_._1)
      input_db_cat
        // Filter processed journeys
        .filter(col(config.sessionIDColNameDbCat).isin(fileNames:_*))
        .repartition(1)
        .write
        .option("header", "true")
        .mode(SaveMode.Append)
        .csv(config.outputDbCat)
    }

    // End timestamp
    val endBatchTimeStamp = System.currentTimeMillis

    // Sleep
    val tmp = config.minBatchTimeSeconds * 1000 - (endBatchTimeStamp - initBatchTimeStamp)
    val stopTime = if(tmp > 0) tmp else 0
    logger.info("Waiting for more over data " + (stopTime/1000).toFloat + " seconds")
    Thread.sleep(stopTime)
  }


}