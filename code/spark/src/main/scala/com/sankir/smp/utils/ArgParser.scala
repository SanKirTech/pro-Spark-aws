/*
 *
 *  * SanKir Technologies
 *  * (c) Copyright 2021.  All rights reserved.
 *  * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  *  without the prior written permission from Sankir Technologies.
 *  *
 *  * The course contents can be accessed by subscribing to pro-Spark course.
 *  *
 *  * Please visit www.sankir.com for details.
 *  *
 *
 */

package com.sankir.smp.utils

import scopt.OParser

object ArgParser {
  private val builder = OParser.builder[CmdLineOptions]

  /***
    * OParser.parse(parser, args, CmdLineOptions()) is invoked by parseLogic() function.
    * parser which is function passed as argument gets invoked and the following code gets execueted
    * Options are compared and its value is populated into CmdLineOptions case object
    * and passed back to ApplicationMain
    */
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

  /***
    *
    * @param args takes in all the arguments string and return a case class object
    * @return o/p of parser if success or if there is failure then exits with status 1
    *         and return CmdLineOptions()
    */
  def parseLogic(args: Array[String]): CmdLineOptions = {
    OParser.parse(parser, args, CmdLineOptions()) match {
      case Some(value) => value
      case None =>
        System.exit(1)
        CmdLineOptions()
    }
  }

}

/***
  *
  * @param schemaLocation - Location of the scheama json which is used as reference
  *                       while doing schema validation
  * @param inputLocation - Location where data is stored.
  * @param projectId  - GCP projectId
  * @param bqDataset  - BigQuery Dataset
  * @param bqTableName  - BigQuery Table name where the results are persisted
  * @param bqErrorTable  - BigQuery Error table where invalid records are stored
  * @param kpiLocation  - Location of JSON containinig KPI
  */
case class CmdLineOptions(schemaLocation: String = "",
                          inputLocation: String = "",
                          projectId: String = "",
                          bqDataset: String = "",
                          bqTableName: String = "",
                          bqErrorTable: String = "",
                          kpiLocation: String = "")
