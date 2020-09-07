package com.sankir.smp.utils

import scopt.OParser

object ArgParser {
  private val builder = OParser.builder[Config]
  private val parser = {
    import builder._
    OParser.sequence(
      opt[String]("projectId")
        .action((x, c) => c.copy(inputLocation = x))
        .required()
        .text("ProjectId"),
      opt[String]("schemaPath")
        .action((x, c) => c.copy(schemaLocation = x))
        .required()
        .text("Schema Path"),
      opt[String]("inputLocation")
        .action((x, c) => c.copy(inputLocation = x))
        .required()
        .text("Input location of Standard Files"),
      opt[String]("bqDataset")
        .action((x, c) => c.copy(inputLocation = x))
        .required()
        .text("BigQuery Dataset"),
      opt[String]("bqTableName")
        .action((x, c) => c.copy(inputLocation = x))
        .required()
        .text("BigQuery Table name"),
      opt[String]("bqErrorTable")
        .action((x, c) => c.copy(inputLocation = x))
        .required()
        .text("BigQuery Error Table Name")
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
        inputLocation: String = "",
        projectId: String = "",
        bqDataset: String = "",
        bqTableName: String = "",
        bqErrorTable: String = ""
      )
