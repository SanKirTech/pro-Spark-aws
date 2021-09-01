package com.sankir.smp.aws

import com.sankir.smp.utils.encoders.CustomEncoders._
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.Converter.convertToErrorTableRows
import com.sankir.smp.gcp.BigQueryIO
import com.sankir.smp.utils.{CloudConnector, CmdLineOptions}
import com.sankir.smp.utils.enums.ErrorEnums.ErrorEnums
import org.apache.spark.sql.Dataset

import scala.util.Try

object AWSConnector extends CloudConnector {
  def createS3Client(): AmazonS3 = {
    AmazonS3ClientBuilder
      .standard()
      .withRegion(Regions.US_EAST_2)
      .build()

  }

  override def read(projectId: String, path: String): String = {
    val s3: AmazonS3 = createS3Client();
    s3.getObjectAsString(projectId, path)
  }

  override def writeError(errorDataSet: Dataset[(String, Try[JsonNode])],
                          configs: CmdLineOptions,
                          jobName: String,
                          errorEnums: ErrorEnums): Unit = {
    errorDataSet
      .map(
        errMsg => convertToErrorTableRows[JsonNode](errMsg, errorEnums, jobName)
      )
      .foreachPartition(tableRows => {
        val bigQueryIO = BigQueryIO(projectId = configs.projectId)
        tableRows.foreach(
          bigQueryIO.insertRow(configs.bqDataset, configs.bqErrorTable, _)
        )
      })
  }

  case class BigTableErrorRows(errorReplay: String = "",
                               timestamp: String,
                               errorType: String,
                               payload: String,
                               jobName: String,
                               errorMessage: String,
                               stackTrace: String)
}
