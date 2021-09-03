package com.sankir.smp.cloud.gcp

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}
import com.google.cloud.storage.{Storage, StorageOptions}
import com.sankir.smp.cloud.CloudConnector
import org.apache.commons.lang3.StringUtils.isNotEmpty
import org.codehaus.jackson.JsonNode

import java.io.FileInputStream
import java.nio.file.{Files, Paths}

case class GcpConnector(configuration: Map[String, Any]) extends CloudConnector {

  val SERVICE_ACCOUNT_PATH = "service_account_path"
  val PROJECT_ID = "project_id"

  var storageClient: Storage = _
  var bigQueryClient: BigQuery = _

  val serviceAccountJson = configuration.get(SERVICE_ACCOUNT_PATH).toString
  val projectId = configuration.get(PROJECT_ID).toString

  if (isNotEmpty(serviceAccountJson) && Files.exists(Paths.get(serviceAccountJson))) {
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

  val gcsIO = GcsIO(storageClient)
  val bigQueryIO = BigQueryIO(bigQueryClient)


  override def saveToObjectStorage(path: String, data: String): Unit = {

  }

  override def readFromObjectStorage(path: String): String = {
    gcsIO.getData(path)
  }

  override def saveToErrorTable(data: JsonNode): Unit = ???

  override def saveToErrorTable(datas: List[JsonNode]): Unit = ???

  override def saveToIngresTable(data: JsonNode): Unit = ???

  override def saveToIngressTable(datas: List[JsonNode]): Unit = ???

}
