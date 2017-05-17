package com.lnicalo.AvroIngestion
import scala.beans.BeanProperty

/**
  * Created by lnicalo on 21/04/17.
  */
class ExportToBQConfig {
  @BeanProperty var inputDir: String = ""
  @BeanProperty var outputDir: String = ""

  @BeanProperty var jsonKeyFile: String = ""
  @BeanProperty var bigQueryProjectId: String = ""
  @BeanProperty var bigQueryGcsBucket: String = ""
  @BeanProperty var bigQueryDatasetLocation: String = ""
  @BeanProperty var checkpointLocation: String = ""
  @BeanProperty var tableReferenceSink: String = ""
  @BeanProperty var timeColName: String = ""

  @BeanProperty var proxyHost: String = ""
  @BeanProperty var proxyPort: String = ""

  override def toString: String = s"inputDir: $inputDir, " +
    s"outputDir: $outputDir, "
}
