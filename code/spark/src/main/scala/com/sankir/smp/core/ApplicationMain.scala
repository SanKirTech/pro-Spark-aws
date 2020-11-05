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

package com.sankir.smp.core

//import com.sankir.smp.core.transformations.ErrorTransformations.writeToBigQuery

import com.sankir.smp.core.transformations.Insight
import com.sankir.smp.core.validators.RetailBusinessValidator
import com.sankir.smp.core.validators.DataValidator.{businessValidator, jsonValidator, schemaValidator}
import com.sankir.smp.utils.ArgParser
import com.sankir.smp.utils.FileSource.readAsStringFromGCS
//import com.sankir.smp.utils.enums.ErrorEnums.{INVALID_JSON_ERROR, SCHEMA_VALIDATION_ERROR}
import org.apache.spark.sql.SparkSession

//import scala.util.Try

// run terraform at D:\1-Data Leap\SparkCode\Testing\sankir-spark-052020\infrastructure\terraforms\project level
object ApplicationMain {
  def main(args: Array[String]): Unit = {

    //    Parsing the Arguments and updating the config object that will help us in providing necessary inputs
    val CMDLINEOPTIONS = ArgParser.parse(args)

    //Reading the schemaString
    val schema = readAsStringFromGCS(CMDLINEOPTIONS.projectId, CMDLINEOPTIONS.schemaLocation)

    //Reading the business Rules
  //  val bussinessRules = readAsStringFromGCS(CMDLINEOPTIONS.projectId, CMDLINEOPTIONS.businessRulesPath).split(",").toList

    // Creating SparkSession
    //val sparkSession = SparkSession.builder().appName("Pro-Spark-Batch").getOrCreate()
    val sparkSession = SparkSession
      .builder()
      .appName("Pro-Spark-Batch")
      .master("local[*]")
      .getOrCreate()

    val JOBNAME = s"${sparkSession.sparkContext.appName}-${sparkSession.sparkContext.applicationId}"

    import com.sankir.smp.utils.encoders.CustomEncoders._

    // Reading data from the input location
    // sdfRecords is the dataset crated from the processedJSON with metadata
    val sdfRecords = sparkSession.read.textFile(CMDLINEOPTIONS.inputLocation)
    println("\n--------  Total sdf Records ------------ " + sdfRecords.count())
    sdfRecords.show(20, false)

    // jsonValidator validates whether the input data is valid or not
    val jsonValidatedRecords = jsonValidator(sdfRecords)
    println("\n--------------- JSON Validated Records -------------")
    jsonValidatedRecords.take(20).foreach(println)

    // get the content wrapped in Success  rec_._2.get does this
    val validJsonRecords = jsonValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    //val validJsonRecords = jsonValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get.get("_p").get("data")))
    println("\n--------------- valid JSON Records --------------- " + validJsonRecords.count())
    validJsonRecords.take(20).foreach(println)


    val invalidJsonRecords = jsonValidatedRecords.filter(_._2.isFailure)
    //writeToBigQuery(invalidJsonRecords, CMDLINEOPTIONS, JOBNAME, INVALID_JSON_ERROR)
    println("\n--------------- invalid JSON Records ------------- " + invalidJsonRecords.count())
    invalidJsonRecords.take(20).foreach(println)

    val schemaValidatedRecords = schemaValidator(validJsonRecords, schema)
    println("\n---------------- Schema Validated Records ------")
    schemaValidatedRecords.take(20).foreach(println)

    val validSchemaRecords = schemaValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    println("\n---------------- valid Schema Records ------  " + validSchemaRecords.count())
    validSchemaRecords.take(20).foreach(println)

    val invalidSchemaRecords = schemaValidatedRecords.filter(_._2.isFailure)
    //writeToBigQuery(invalidSchemaRecords, CMDLINEOPTIONS, JOBNAME, INVALID_SCHEMA_ERROR)
    println("\n---------------- invalid Schema Records ------ " + invalidSchemaRecords.count())
    invalidSchemaRecords.take(20).foreach(println)

    val businessValidatedRecords = businessValidator(validSchemaRecords, RetailBusinessValidator.validate)

    val validBusinessRecords = businessValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    println("\n---------------- valid BizData Records ------ " + validBusinessRecords.count())
    println(" Count : " + validBusinessRecords.count())
    validBusinessRecords.take(20).foreach(println)

    val invalidBusinessRecords = businessValidatedRecords.filter(_._2.isFailure)
//    writeToBigQuery(invalidSchemaRecords, CMDLINEOPTIONS, JOBNAME, INVALID_BIZ_DATA)
    println("\n---------------- invalid BizData Records ------ " + invalidBusinessRecords.count())
    println(" Count : " + invalidBusinessRecords.count())
    invalidBusinessRecords.take(20).foreach(println)

    println("\n---------------- retailDF with retailaSchema field types matched------")
    val retailDF = sparkSession.read.schema(Insight.retailSchema).json(validBusinessRecords.map(_._2.toString))

    retailDF.printSchema()
    retailDF.show(20, false)

  }
}
