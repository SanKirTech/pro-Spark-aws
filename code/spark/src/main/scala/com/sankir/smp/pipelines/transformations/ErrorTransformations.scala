package com.sankir.smp.pipelines.transformations

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.converters.Converter.convertToErrorTableRows
import com.sankir.smp.connectors.BigQueryIO
import com.sankir.smp.utils.Config
import com.sankir.smp.utils.enums.ErrorEnums.ErrorEnums
import org.apache.spark.sql.Dataset

import scala.util.Try

object ErrorTransformations {
  def writeToBigQuery(errorDataSet: Dataset[(String, Try[JsonNode])], configs: Config, jobName: String, errorEnums: ErrorEnums): Unit = {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    errorDataSet.map(errMsg =>
      convertToErrorTableRows[JsonNode](errMsg, errorEnums, jobName))
      .foreachPartition(tableRows => {
        val bigQueryIO = BigQueryIO(projectId = configs.projectId)
        bigQueryIO.insertIterableRows(configs., configs.bqErrorTable, tableRows.toIterable)
      })
  }
}
