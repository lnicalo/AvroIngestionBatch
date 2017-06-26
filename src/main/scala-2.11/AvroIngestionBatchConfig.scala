package com.lnicalo.AvroIngestion

import scala.beans.BeanProperty

/**
  * Created by lnicalo on 06/04/17.
  */
class AvroIngestionBatchConfig {
  @BeanProperty var inputDir: String = ""
  @BeanProperty var inputDbCat: String = ""

  @BeanProperty var outputDir: String = ""
  @BeanProperty var outputDbCat: String = ""
  @BeanProperty var outputSignalMap: String = ""
  @BeanProperty var tmpDir: String = ""

  @BeanProperty var maxBatchSizeMillonOfMessages: Long = 0
  @BeanProperty var minBatchTimeSeconds: Long = 0

  @BeanProperty var sessionIDColNameDbCat: String = ""
  @BeanProperty var moduleColName: String = ""
  @BeanProperty var filesizeColName: String = ""

  override def toString: String = s"inputDir: $inputDir, " +
    s"inputDbCat: $inputDbCat, " +
    s"outputDir: $outputDir, " +
    s"outputDbCat: $outputDbCat, " +
    s"maxBatchSizeBytes: $maxBatchSizeMillonOfMessages, " +
    s"minBatchTimeSeconds: $minBatchTimeSeconds"
}