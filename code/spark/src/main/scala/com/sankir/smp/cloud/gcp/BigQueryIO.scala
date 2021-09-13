package com.sankir.smp.cloud.gcp

import com.google.api.services.bigquery.model.TableRow
import com.google.cloud.bigquery.{BigQuery, InsertAllRequest, TableId}
import org.apache.spark.internal.Logging
import org.slf4j.LoggerFactory

final case class BigQueryIO(bigQueryClient: BigQuery) extends Logging {

  /**
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
      logError(
        s"Error inserting data to ${tableId.getDataset} ${tableId.getTable}"
      )
//    else
//      logInfo(s"Data inserted in ${tableId.getDataset} ${tableId.getTable}")
  }

}
