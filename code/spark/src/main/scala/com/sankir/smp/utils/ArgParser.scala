/*
 * SanKir Technologies
 * (c) Copyright 2020.  All rights reserved.
 * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  without the prior written permission from Sankir Technologies.
 *
 * The course contents can be accessed by subscribing to pro-Spark course.
 *
 * Please visit www.sankir.com for details.
 *
 */

package com.sankir.smp.utils

import scopt.OParser

object ArgParser {
  private val builder = OParser.builder[CmdLineOptions]
  private val parser = {
    import builder._
    OParser.sequence(
      opt[String]("projectId")
        .action((x, c) => c.copy(projectId = x))
        .required()
        .text("ProjectId"),
      opt[String]("schemaLocation")
        .action((x, c) => c.copy(schemaLocation = x))
        .required()
        .text("Schema Path"),
//      opt[String]("businessRules")
//        .action((x, c) => {
//          c.copy(businessRulesPath = x)
//        })
//        .required()
//        .text("Business Rules Path"),
      opt[String]("inputLocation")
        .action((x, c) => c.copy(inputLocation = x))
        .required()
        .text("Input location of Standard Files"),
      opt[String]("bqDataset")
        .action((x, c) => c.copy(bqDataset = x))
        .required()
        .text("BigQuery Dataset"),
      opt[String]("bqTableName")
        .action((x, c) => c.copy(bqTableName = x))
        .required()
        .text("BigQuery Table name"),
      opt[String]("bqErrorTable")
        .action((x, c) => c.copy(bqErrorTable = x))
        .required()
        .text("BigQuery Error Table Name"),
      opt[String]("kpiLocation")
        .action((x, c) => c.copy(kpiLocation = x))
        .required()
        .text("kpi json Path")
    )
  }

  def parse(args: Array[String]): CmdLineOptions = {
    OParser.parse(parser, args, CmdLineOptions()) match {
      case Some(value) => value
      case None => System.exit(1)
        CmdLineOptions()
    }
  }


}

//  All these are populated from command line arguments
case class
CmdLineOptions(
        schemaLocation: String = "",
      //  businessRulesPath: String = "",
        inputLocation: String = "",
        projectId: String = "",
        bqDataset: String = "",
        bqTableName: String = "",
        bqErrorTable: String = "",
        kpiLocation: String = ""
      )
