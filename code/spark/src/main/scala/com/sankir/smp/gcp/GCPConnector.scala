package com.sankir.smp.gcp

import java.io.InputStream

import com.sankir.smp.utils.encoders.CustomEncoders._
import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.Converter.convertToErrorTableRows
import com.sankir.smp.utils.{CloudConnector, CmdLineOptions}
import com.sankir.smp.utils.enums.ErrorEnums.ErrorEnums
import org.apache.spark.sql.Dataset

import scala.util.Try

object GCPConnector extends CloudConnector {

  def readAsStringIterator(path: String): Iterator[String] = {
    scala.io.Source.fromInputStream(read(path)).getLines()
  }

  def readAsString(path: String): String = {
    readAsStringIterator(path).mkString("\n")
  }

  def read(path: String): InputStream =
    GCPConnector.getClass.getClassLoader.getResourceAsStream(path)

  /***
    *
    * @param projectId - Pass the projectid which you have created in GCP
    * @param path  - Path of the schema json which is used as reference to validate the schema
    * @return return the content of the file as string
    */
  override def read(projectId: String, path: String): String = {
    val gcsIO = GcsIO(projectId = projectId)
    gcsIO.getData(path)
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
