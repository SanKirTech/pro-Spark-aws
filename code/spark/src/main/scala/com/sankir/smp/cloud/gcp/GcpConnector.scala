package com.sankir.smp.cloud.gcp

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import com.google.cloud.storage.{Storage, StorageOptions}
import com.sankir.smp.cloud.{CloudConfig, CloudConnector}
import com.sankir.smp.core.transformations.Insight.RetailCase
import org.apache.commons.lang3.StringUtils.isNotEmpty
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.codehaus.jackson.JsonNode

import java.io.FileInputStream
import java.nio.file.{Files, Paths}

abstract final case class GcpConnector(cloudConfig: CloudConfig,
                                       configuration: Map[String, Any])
    extends CloudConnector {

  private val SERVICE_ACCOUNT_PATH = "service_account_path"
  private val PROJECT_ID = "project_id"
  private val TEMP_GCS_BUCKET = "tempGCSBucket"

  private var storageClient: Storage = _
  private var bigQueryClient: BigQuery = _

  private val serviceAccountJson =
    configuration.get(SERVICE_ACCOUNT_PATH).toString
  private val projectId = configuration.get(PROJECT_ID).toString

  if (isNotEmpty(serviceAccountJson) && Files.exists(
        Paths.get(serviceAccountJson)
      )) {
    val serviceAccountCredentials =
      GoogleCredentials.fromStream(new FileInputStream(serviceAccountJson))

    storageClient = StorageOptions
      .newBuilder()
      .setCredentials(serviceAccountCredentials)
      .setProjectId(projectId)
      .build()
      .getService

    bigQueryClient = BigQueryOptions
      .newBuilder()
      .setCredentials(serviceAccountCredentials)
      .setProjectId(projectId)
      .build()
      .getService

  } else {
    storageClient = StorageOptions.getDefaultInstance.getService
    bigQueryClient = BigQueryOptions.getDefaultInstance.getService
  }

  private val gcsIO = GcsIO(storageClient)
  private val bigQueryIO = BigQueryIO(bigQueryClient)

  override def saveToObjectStorage(path: String, data: String): Unit = {}

  override def readFromObjectStorage(path: String): String = {
    gcsIO.getData(path)
  }

  override def saveToErrorTable(data: JsonNode): Unit = ???

  override def saveToErrorTable(datas: List[JsonNode]): Unit = ???

  override def saveToIngressTable(data: JsonNode): Unit = ???

  override def saveToIngressTable(datas: List[JsonNode]): Unit = ???

  //override def saveToErrorTable(df: DataFrame): Unit = ???

//  override def saveToIngressTable(goodRecords: DataFrame): Unit = {
//    val bucket = configuration.get(TEMP_GCS_BUCKET).toString
//    val ingressTable = cloudConfig.ingressTable
//    goodRecords.sparkSession.conf.set("temporaryGcsBucket", bucket)
//    goodRecords
//      .coalesce(5)
//      .write
//      .format("bigquery")
//      .mode("append")
//      .save(ingressTable)
//  }

  override def saveToIngressTable[T](ds: Dataset[T]): Unit = {
    val bucket = configuration.get(TEMP_GCS_BUCKET).toString
    val ingressTable = cloudConfig.ingressTable
    ds.sparkSession.conf.set("temporaryGcsBucket", bucket)
    ds.coalesce(5)
      .write
      .format("bigquery")
      .mode("append")
      .save(ingressTable)
  }
}
