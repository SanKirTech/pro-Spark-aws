package com.sankir.smp.utils

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.utils.enums.ErrorEnums.ErrorEnums
import org.apache.spark.sql.Dataset

import scala.util.Try

trait CloudConnector {
  def read(projectId: String, schemaLocation: String): String
  def writeError(invalidJsonRecords: Dataset[(String, Try[JsonNode])],
                 cmdlineoptions: CmdLineOptions,
                 jobname: String,
                 invalidJsonEnum: ErrorEnums): Unit
}
