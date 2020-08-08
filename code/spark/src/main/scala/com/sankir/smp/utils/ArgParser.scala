package com.sankir.smp.utils

import scopt.OParser

object ArgParser {
  private val builder = OParser.builder[Config]
  private val parser = {
    import builder._
    OParser.sequence(
      opt[String]("schemaValidationPath")
        .action((x, c) => c.copy(schemaLocation = x))
        .text("Schema Validation Path"),
      opt[String]("inputLocation")
        .action((x, c) => c.copy(inputLocation = x))
    )
  }

  def parse(args: Array[String]): Config = {
    OParser.parse(parser, args, Config()) match {
      case Some(value) => value
      case None => System.exit(1)
        Config()
    }
  }


}

case class
Config(
        schemaLocation: String = "",
        inputLocation:String = ""
      )
