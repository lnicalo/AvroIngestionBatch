package com.lnicalo.AvroIngestion

import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
  * Created by lnicalo on 06/04/17.
  */
class ArgParser(arguments: Seq[String]) extends ScallopConf(arguments) {
  val yaml: ScallopOption[String] = opt[String](required = true)
  verify()
}
