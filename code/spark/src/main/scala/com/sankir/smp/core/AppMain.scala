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
import com.sankir.smp.cloud.{CloudConfig, CloudConnector}
import com.sankir.smp.core.transformations.Insight.RetailCase
import com.sankir.smp.core.validators.RetailBusinessValidator
import com.sankir.smp.core.validators.DataValidator.{businessValidator, jsonValidator, schemaValidator}
import org.apache.spark.sql.Dataset

import scala.util.Try
import org.apache.spark.sql.SparkSession

object AppMain {
  def run(args: Array[String], cloudConnector: CloudConnector, cloudConfig: CloudConfig): Unit = {
    //Read the command line arguments
    //Check Case class CmdLineOptions in ArgParser to know the command line args

    /***
      * schema json is stored in t_transaction.json.  The following code accesses the
      * GCS storage and gets it back in the form of string for
      * downstream utility such as schema validation
      */
    val schema = cloudConnector.readFromObjectStorage(cloudConfig.schemaLocation)
    println(schema)

    val sparkSession = SparkSession
      .builder()
      .appName("Pro-Spark-Batch")
      .master("local[*]")
      .getOrCreate();

//    sparkSession.read
//      .json("s3a://retail-sankir/processed-retail-data/q1/2011-01-04.json")
    val JOBNAME =
      s"${sparkSession.sparkContext.appName}-${sparkSession.sparkContext.applicationId}"

    import com.sankir.smp.utils.encoders.CustomEncoders._

    println("==================================")
    println(cloudConfig.inputLocation)
    println("==================================")
    val sdfRecords = sparkSession.read.textFile(cloudConfig.inputLocation)
    //println("\n--------  Total sdf Records ------------ " + sdfRecords.count())
    //sdfRecords.show(20, false)

    val jsonValidatedRecords = jsonValidator(sdfRecords)
    println("\n--------------- JSON Validated Records -------------")
    jsonValidatedRecords.take(20).foreach(println)

    // get the content wrapped in Success  rec_._2.get does this
    val validJsonRecords = jsonValidatedRecords
      .filter(_._2.isSuccess)
      .map(rec => (rec._1, rec._2.get))

    //val validJsonRecords = jsonValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get.get("_p").get("data")))
    println(
      "\n--------------- valid JSON Records --------------- " + validJsonRecords
        .count()
    )
    validJsonRecords.take(20).foreach(println)

    val invalidJsonRecords = jsonValidatedRecords.filter(_._2.isFailure)

    /*************************
      * Schema Validation Begins
      * * **********************
      * function: schemaValidator
      *  @param validJsonRecords
      *  @param schema
      *  @return Dataset[Jsonstring, Try[JsonNode] - Here Jsonstring is the actual record and
      *          Try[JsonNode] returns either Success[JsonNode] or Failure[JsonNode]
      *   Success records will be used further for Biz Validation
      *   Failure records will be inserted into bigquery error table
      * */
    /*
      To see code of schemaValidator - hold the ctrl key and click on schemaValidator
     */
    val schemaValidatedRecords: Dataset[(String, Try[JsonNode])] =
      schemaValidator(validJsonRecords, schema)
    println("\n---------------- Schema Validated Records ------")
    schemaValidatedRecords.take(20).foreach(println)

    val validSchemaRecords = schemaValidatedRecords
      .filter(_._2.isSuccess)
      .map(rec => (rec._1, rec._2.get))
    println(
      "\n---------------- valid Schema Records ------  " + validSchemaRecords
        .count()
    )
    validSchemaRecords.take(20).foreach(println)

    val invalidSchemaRecords = schemaValidatedRecords.filter(_._2.isFailure)
//    writeToBigQuery(
    ////      invalidSchemaRecords,
    ////      CMDLINEOPTIONS,
    ////      JOBNAME,
    ////      INVALID_SCHEMA
    //)

    println(
      "\n---------------- invalid Schema Records ------ " + invalidSchemaRecords
        .count()
    )
    invalidSchemaRecords.take(20).foreach(println)

    /* Schema Validation Ends */

    /***
      * Business Validation Begins
      * function: businessValidator
      *
      * @param validSchemaRecords json records which have passed jsonvalidation and schemaValidation
      * @param RetailBusinessValidator.validate - function which has business rules defined.
      *  @return Dataset[Jsonstring, Try[JsonNode] - Here Jsonstring is the actual record and
      *          Try[JsonNode] returns either Success[JsonNode] or Failure[JsonNode]
      *          Success records will be used further for kpi generation
      *          Failure records will be inserted into bigquery error table
      */
    /*
         To see code of RetailBusinessValidator.validate - hold the ctrl key and click on schemaValidator
     */
    val businessValidatedRecords: Dataset[(String, Try[JsonNode])] =
      businessValidator(validSchemaRecords, RetailBusinessValidator.validate)
    println("\n---------------- Business Validated Records ------")
    businessValidatedRecords.take(20).foreach(println)

    val validBusinessRecords = businessValidatedRecords
      .filter(_._2.isSuccess)
      .map(rec => (rec._1, rec._2.get))
    println(
      "\n---------------- valid BizData Records ------ " + validBusinessRecords
        .count()
    )
    println(" Count : " + validBusinessRecords.count())
    validBusinessRecords.take(20).foreach(println)

    val invalidBusinessRecords = businessValidatedRecords.filter(_._2.isFailure)

//    writeToBigQuery(
//      invalidBusinessRecords,
//      CMDLINEOPTIONS,
//      JOBNAME,
//      INVALID_BIZ_DATA
//    )

    println(
      "\n---------------- invalid BizData Records ------ " + invalidBusinessRecords
        .count()
    )
    println(" Count : " + invalidBusinessRecords.count())
    invalidBusinessRecords.take(20).foreach(println)

    println(
      "\n---------------- retailDS with retailaSchema field types matched------"
    )
    import sparkSession.implicits._

    //Dataset retailDS is created from the validBusinessRecords JsonNode
    val retailDS = sparkSession.read
      .json(validBusinessRecords.map(_._2.toString))
      .as[RetailCase]

    //    val ingressTable = CMDLINEOPTIONS.bqDataset + "." + CMDLINEOPTIONS.bqTableName
    //    Insight.writeToIngress(sparkSession, retailDS, ingressTable)

    retailDS.printSchema()
    retailDS.show(20, false)

    println("\n----------------Spark sql table retail_tbl------")
    retailDS.createOrReplaceGlobalTempView("retail_tbl")

    val sparkTable = "global_temp.retail_tbl"

//    Insight.runKPIQuery(sparkSession, sparkTable, cloudConfig.kpiLocation)
  }
}
