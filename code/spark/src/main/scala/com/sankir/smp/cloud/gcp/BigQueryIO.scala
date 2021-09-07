package com.sankir.smp.cloud.gcp

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.bigquery.{BigQuery, InsertAllRequest, TableId}
import org.slf4j.LoggerFactory

case class BigQueryIO(bigQueryClient: BigQuery) {
  private val LOG = LoggerFactory.getLogger(BigQueryIO.getClass)

  /** *
    *
    * @param dataset - BigQuery schema
    * @param table   - Table that needs to be inserted with
    * @param rows    - rows of records that need to be inserted
    */
  def insertRow(dataset: String, table: String, rows: TableRow): Unit = {

    val tableId = TableId.of(dataset, table)
    val response = bigQueryClient.insertAll(
      InsertAllRequest
        .newBuilder(tableId)
        .addRow(rows)
        .build()
    )
    if (response.hasErrors)
      LOG.error(
        s"Error inserting data to ${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}"
      )
    else
      LOG.info(
        s"Data inserted in ${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}"
      )

  }

}
