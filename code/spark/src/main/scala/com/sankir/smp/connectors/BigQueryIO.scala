package com.sankir.smp.connectors

import com.google.api.services.bigquery.model.TableRow
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.{BigQueryOptions, InsertAllRequest, TableId}
import org.slf4j.LoggerFactory

case class BigQueryIO(var googleCredentials: ServiceAccountCredentials = null, projectId: String) {
  val LOG = LoggerFactory.getLogger(BigQueryIO.getClass)


  val bigQueryIO =
    if (googleCredentials ne null)
      BigQueryOptions.newBuilder()
        .setCredentials(googleCredentials)
        .setProjectId(projectId)
        .build()
        .getService
    else BigQueryOptions.getDefaultInstance.getService

  def insertRow( dataset: String, table: String, rows: TableRow) = {
    val tableId = TableId.of(dataset,table)
    val response = bigQueryIO.insertAll(
      InsertAllRequest
        .newBuilder(tableId)
        .addRow(rows)
        .build())
    if (response.hasErrors)
      LOG.error(s"Error inserting data to ${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}")
    else
      LOG.info(s"Data inserted in ${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}")

  }


}


