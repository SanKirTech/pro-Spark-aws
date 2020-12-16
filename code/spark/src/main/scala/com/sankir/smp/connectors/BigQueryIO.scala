/*
 * SanKir Technologies
 * (c) Copyright 2020.  All rights reserved.
 * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  without the prior written permission from Sankir Technologies.
 *
 * The course contents can be accessed by subscribing to pro-Spark course.
 *
 * Please visit www.sankir.com for details.
 *
 */

package com.sankir.smp.connectors

import com.google.api.services.bigquery.model.TableRow
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery.{BigQueryOptions, InsertAllRequest, TableId}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConverters._

case class BigQueryIO(var googleCredentials: ServiceAccountCredentials = null,
                      projectId: String) {
  private val LOG = LoggerFactory.getLogger(BigQueryIO.getClass)

  /***
    * Create a bigQueryIO object instance which gives access to insert methods
    */
  val bigQueryIO =
    if (googleCredentials ne null)
      BigQueryOptions
        .newBuilder()
        .setCredentials(googleCredentials)
        .setProjectId(projectId)
        .build()
        .getService
    else BigQueryOptions.getDefaultInstance.getService

  /***
    *
    * @param dataset - BigQuery schema
    * @param table - Table that needs to be inserted with
    * @param rows - rows of records that need to be inserted
    */
  def insertIterableRows(dataset: String,
                         table: String,
                         rows: Iterable[TableRow]) = {
    val tableId = TableId.of(dataset, table)
    val response = bigQueryIO.insertAll(
      InsertAllRequest
        .newBuilder(tableId)
        .setRows(rows.map(InsertAllRequest.RowToInsert.of(_)).asJava)
        .build()
    )
    if (response.hasErrors)
      LOG.error(s"""
           |Error inserting data to ${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}
           |${response.getInsertErrors.mkString("\n")}""".stripMargin)
    else
      LOG.info(
        s"Data inserted in ${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}"
      )
  }

  /***
    *
    * @param dataset - BigQuery schema
    * @param table - Table that needs to be inserted with
    * @param rows - rows of records that need to be inserted
    */
  def insertRow(dataset: String, table: String, rows: TableRow) = {

    val tableId = TableId.of(dataset, table)
    val response = bigQueryIO.insertAll(
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
