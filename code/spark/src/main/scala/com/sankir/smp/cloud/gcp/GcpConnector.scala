package com.sankir.smp.cloud.gcp

import com.google.api.services.bigquery.model.TableRow
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.storage.{Storage, StorageOptions}
import com.sankir.smp.cloud.CloudConnector
import com.sankir.smp.cloud.common.vos.{CloudConfig, ErrorTableRow}
import com.sankir.smp.common.Matchers.and
import com.sankir.smp.common.{JsonUtils, Matcher, Matchers}
import com.sankir.smp.utils.encoders.CustomEncoders.tableRowEncoder
import org.apache.commons.lang3.StringUtils.isNotEmpty
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.io.FileInputStream
import java.nio.file.{Files, Paths}

final case class GcpConnector(cloudConfig: CloudConfig,
                              configuration: Map[String, Any])
  extends CloudConnector
    with Logging
    with Serializable {

  private val SERVICE_ACCOUNT_PATH = "serviceAaccountPath"
  private val PROJECT_ID = "project_id"
  private val TEMP_GCS_BUCKET = "tempGCSBucket"

  private val serviceAccountJson =
    configuration.getOrElse(SERVICE_ACCOUNT_PATH, "").toString
  private val projectId = configuration.get(PROJECT_ID).toString

  private val serviceAccountFileExistsMatcher: Matcher[String] =
    and(new Matcher[String] {
      override def test(t: String): Boolean = isNotEmpty(t)
    }, new Matcher[String] {
      override def test(t: String): Boolean = Files.exists(Paths.get(t))
    })

  private def createStorageClient: Storage = {
    if (serviceAccountFileExistsMatcher.test(serviceAccountJson)) {
      val serviceAccountCredentials =
        GoogleCredentials.fromStream(new FileInputStream(serviceAccountJson))
      StorageOptions
        .newBuilder()
        .setCredentials(serviceAccountCredentials)
        .setProjectId(projectId)
        .build()
        .getService
    } else {
      StorageOptions.getDefaultInstance.getService
    }
  }

  private def createBigQueryIO: BigQueryIO = {
    BigQueryIO(if (serviceAccountFileExistsMatcher.test(serviceAccountJson)) {
      val serviceAccountCredentials =
        GoogleCredentials.fromStream(new FileInputStream(serviceAccountJson))
      BigQueryOptions
        .newBuilder()
        .setCredentials(serviceAccountCredentials)
        .setProjectId(projectId)
        .build()
        .getService
    } else {
      BigQueryOptions.getDefaultInstance.getService
    })
  }

  override def readFromObjectStorage(path: String): String = {
    val gcsIO = GcsIO(createStorageClient)
    gcsIO.getData(path)
  }

  override def saveToIngressTable[T](ds: Dataset[T]): Unit = {
    val bucket = configuration(TEMP_GCS_BUCKET).toString
    val ingressTable = cloudConfig.ingressTable
    ds.sparkSession.conf.set("temporaryGcsBucket", bucket)
    ds.coalesce(5)
      .write
      .format("bigquery")
      .mode("append")
      .save(ingressTable)
    logInfo(s"Data saved to ${cloudConfig.ingressTable}")
  }

  override def saveToErrorTable(ds: Dataset[ErrorTableRow]): Unit = {
    val dataset = cloudConfig.errorTable.split("\\.")(0)
    val table = cloudConfig.errorTable.split("\\.")(1)
    ds.map(JsonUtils.MAPPER.convertValue(_, classOf[TableRow]))
      .foreachPartition(rows => {
        val bigQueryIO = createBigQueryIO
        rows.foreach({
          bigQueryIO.insertRow(dataset, table, _)
        })
      })
  }
  override def saveToKPITable(df: DataFrame,
                              kpiTable: String
                              ): Unit = {

    val bucket = configuration(TEMP_GCS_BUCKET).toString
    df.sparkSession.conf.set("temporaryGcsBucket", bucket)
    df.coalesce(5)
      .write
      .format("bigquery")
      .mode("append")
      .save(kpiTable)
   // logInfo(s"Data saved to ${kpiTable}")

  }

  override def saveToKPILocation(df: DataFrame, kpiLocation: String): Unit = ???
}
