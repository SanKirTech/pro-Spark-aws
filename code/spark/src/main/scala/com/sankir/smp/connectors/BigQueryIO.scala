package com.sankir.smp.connectors

import com.google.api.services.bigquery.model.TableRow
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.{BigQueryOptions, InsertAllRequest, TableId}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

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

  def insertIterableRows(dataset: String, table: String, rows: Iterable[TableRow]) = {
    val tableId = TableId.of(dataset, table)
    val response = bigQueryIO.insertAll(
      InsertAllRequest
        .newBuilder(tableId)
        .setRows(rows.map(InsertAllRequest.RowToInsert.of(_)).asJava)
        .build())
    if (response.hasErrors)
      LOG.error(s"Error inserting data to ${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}")
    else
      LOG.info(s"Data inserted in ${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}")
  }

  def insertRow(dataset: String, table: String, rows: TableRow) = {

    val tableId = TableId.of(dataset, table)
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


