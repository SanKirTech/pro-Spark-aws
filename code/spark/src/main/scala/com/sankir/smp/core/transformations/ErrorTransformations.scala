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

package com.sankir.smp.core.transformations

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.Converter.convertToErrorTableRows
import com.sankir.smp.connectors.BigQueryIO
import com.sankir.smp.utils.CmdLineOptions
import com.sankir.smp.utils.enums.ErrorEnums.ErrorEnums
import org.apache.spark.sql.Dataset

import scala.util.Try

object ErrorTransformations {
  def writeToBigQuery(errorDataSet: Dataset[(String, Try[JsonNode])], configs: CmdLineOptions, jobName: String, errorEnums: ErrorEnums): Unit = {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    errorDataSet.map(errMsg =>
      convertToErrorTableRows[JsonNode](errMsg, errorEnums, jobName))
      .foreachPartition(tableRows => {
        val bigQueryIO = BigQueryIO(projectId = configs.projectId)
        bigQueryIO.insertIterableRows(configs.bqDataset, configs.bqErrorTable, tableRows.toIterable)
      })
  }
}
