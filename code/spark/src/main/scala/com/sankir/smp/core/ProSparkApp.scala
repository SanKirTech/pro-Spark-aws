  /*
   *
   *  * SanKir Technologies
   *  * (c) Copyright 2021.  All rights reserved.
   *  * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
   *  *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
   *  *  without the prior written permission from Sankir Technologies.
   *  *
   *  * The course contents can be accessed by subscribing to pro-Spark course.
   *  *
   *  * Please visit www.sankir.com for details.
   *  *
   *
   */

  package com.sankir.smp.core

  import com.fasterxml.jackson.databind.JsonNode
  import com.sankir.smp.cloud.common.{CloudConnector, CloudConverter}
  import com.sankir.smp.cloud.common.vos.CloudConfig
  import com.sankir.smp.cloud.common.vos.RetailCase
  import com.sankir.smp.core.validators.DataValidator.{businessValidator, jsonValidator, schemaValidator}
  import com.sankir.smp.core.validators.RetailBusinessValidator
  import com.sankir.smp.utils.LogFormatter.{formatHeader, formatLogger}
  import org.apache.spark.sql.Dataset
  import com.sankir.smp.core.transformations.Insight

  import scala.util.Try
  import com.sankir.smp.utils.encoders.CustomEncoders._
  import com.sankir.smp.utils.enums.ErrorEnums.{INVALID_BIZ_DATA, INVALID_JSON, INVALID_SCHEMA}
  import org.apache.spark.internal.Logging
  import org.apache.spark.sql.SparkSession

  /**
   * `ProSparkApp` will do the following
   * <ol>
   * <li>Read the schema form cloud object storage
   * <li>Create Spark Session
   * <li>Read the data from the input location into spark objects
   * <li>Validate if it is proper data
   * <li>Validate if the json is following proper schema
   * <li>Validate if the data is aligned with the business rules
   * <li>Store the error data in tables
   * <li>Store the ingress data in tables
   * <li>Run the KPI queries
   * <li>Strore the KPI queries in tables
   * </ol>
   */
  object ProSparkApp extends Logging {

    def run(args: Array[String],
            cloudConnector: CloudConnector,
            cloudConfig: CloudConfig): Unit = {
      // Read the schema from cloud storage
      val schema =
        cloudConnector.readFromObjectStorage(cloudConfig.schemaLocation)
      logDebug(schema)

      // Create Spark session
      val sparkSessionBuilder = SparkSession
        .builder()
        .appName("Pro-Spark-Batch")

      if (cloudConfig.runLocal)
        sparkSessionBuilder.master("local[*]")
      else
        sparkSessionBuilder.master("yarn")

      if (cloudConfig.sparkConfig.nonEmpty)
        cloudConfig.sparkConfig.foreach(kv=> sparkSessionBuilder.config(kv._1,kv._2))

      val sparkSession = sparkSessionBuilder.getOrCreate()
      logInfo("Spark session Created")

      // JobName created to identify the uniquness of each run. Its reference is used in error table
      val JOBNAME =
        s"${sparkSession.sparkContext.appName}-${sparkSession.sparkContext.applicationId}"

      logInfo(s"Input file location: ${cloudConfig.inputLocation}")
      // Read the data from the input location into spark objects
      val sdfRecords = sparkSession.read.textFile(cloudConfig.inputLocation)

      // Validate if it is proper data
      val jsonValidatedRecords = jsonValidator(sdfRecords)
      logInfo(formatHeader("JSON Validated Records"))
      logDebug(dataSetAsString(jsonValidatedRecords))

      // get the content wrapped in Success  rec_._2.get does this
      val validJsonRecords = jsonValidatedRecords
        .filter(_._2.isSuccess)
        .map(rec => (rec._1, rec._2.get))

      logInfo(formatHeader(s" Valid JSON Records : ${validJsonRecords.count()} "))
      logDebug(dataSetAsString(validJsonRecords))

      val invalidJsonRecords = jsonValidatedRecords.filter(_._2.isFailure)
      cloudConnector
        .saveError(
          invalidJsonRecords
            .map(CloudConverter.convertToErrorTableRow(_, INVALID_JSON, JOBNAME))
        )

      val schemaValidatedRecords: Dataset[(String, Try[JsonNode])] =
        schemaValidator(validJsonRecords, schema)
      logInfo(formatHeader("Schema Validated Records"))
      logDebug(dataSetAsString(schemaValidatedRecords))

      val validSchemaRecords = schemaValidatedRecords
        .filter(_._2.isSuccess)
        .map(rec => (rec._1, rec._2.get))
      logInfo(
        formatHeader(s"valid Schema Records: ${validSchemaRecords.count()}")
      )
      logDebug(dataSetAsString(validSchemaRecords))

      val invalidSchemaRecords = schemaValidatedRecords.filter(_._2.isFailure)
      // Save invalid schema records to error Table
      cloudConnector
        .saveError(
          invalidSchemaRecords
            .map(
              CloudConverter.convertToErrorTableRow(_, INVALID_SCHEMA, JOBNAME)
            )
        )

      logInfo(
        formatHeader(s"invalid Schema Records: ${invalidSchemaRecords.count()}")
      )
      logDebug(dataSetAsString(invalidSchemaRecords))

      /* Schema Validation Ends */

      val businessValidatedRecords: Dataset[(String, Try[JsonNode])] =
        businessValidator(validSchemaRecords, RetailBusinessValidator.validate)
      logInfo(formatHeader("Business Validated Records"))
      logDebug(dataSetAsString(businessValidatedRecords))

      val validBusinessRecords = businessValidatedRecords
        .filter(_._2.isSuccess)
        .map(rec => (rec._1, rec._2.get))
      logInfo(
        formatHeader(s"valid BizData Records: ${validBusinessRecords.count()}")
      )
      logDebug(dataSetAsString(validBusinessRecords))

      val invalidBusinessRecords = businessValidatedRecords.filter(_._2.isFailure)

      // Save invalid Business Data to error Table
      cloudConnector
        .saveError(
          invalidBusinessRecords
            .map(
              CloudConverter.convertToErrorTableRow(_, INVALID_BIZ_DATA, JOBNAME)
            )
        )

      logInfo(
        formatHeader(
          s"invalid BizData Records: ${invalidBusinessRecords.count()}"
        )
      )
      logDebug(dataSetAsString(invalidBusinessRecords))

      logInfo(formatHeader("retailDS with retail Schema field types matched"))
      import sparkSession.implicits._

      //Dataset retailDS is created from the validBusinessRecords JsonNode
      val retailDS = sparkSession.read
        .json(validBusinessRecords.map(_._2.toString))
        .as[RetailCase]

      cloudConnector.saveIngress[RetailCase](retailDS)

      logInfo(formatLogger(retailDS.schema.treeString))
      retailDS.show(20, false)

      logInfo(formatHeader("Spark sql table retail_tbl"))
      retailDS.createOrReplaceGlobalTempView(cloudConfig.tempKPIViewName)

      val sparkTable = s"global_temp.${cloudConfig.tempKPIViewName}"

      Insight.initialize(cloudConnector)
      Insight.runKPIQuery(sparkSession, sparkTable, cloudConfig.kpiLocation)
    }

    private def dataSetAsString[T](ds: Dataset[T], num: Int = 20): String = {
      ds.take(20).mkString("\n")
    }
  }
