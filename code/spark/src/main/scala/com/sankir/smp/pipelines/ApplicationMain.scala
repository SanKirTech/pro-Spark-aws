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

package com.sankir.smp.pipelines

//import com.sankir.smp.pipelines.transformations.ErrorTransformations.writeToBigQuery
import com.sankir.smp.pipelines.transformations.Insight
import com.sankir.smp.pipelines.validators.Validator.{jsonValidator, schemaValidator}
import com.sankir.smp.utils.ArgParser
import com.sankir.smp.utils.Resources.readAsStringFromGCS
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

    // Creating SparkSession
    //val sparkSession = SparkSession.builder().appName("Pro-Spark-Batch").getOrCreate()
    val sparkSession = SparkSession.builder().appName("Pro-Spark-Batch").master("local[*]").getOrCreate()

    val JOBNAME = s"${sparkSession.sparkContext.appName}-${sparkSession.sparkContext.applicationId}"

    import com.sankir.smp.utils.encoders.CustomEncoders._

    // Reading data from the input location
    // sdfRecords is the dataset crated from the processedJSON with metadata
    val sdfRecords = sparkSession.read.textFile(CMDLINEOPTIONS.inputLocation)
    println("\n--------  sdf Records ------------")
    sdfRecords.show(2, false)

    // jsonValidator validates whether the input data is valid or not
    val jsonValidatedRecords = jsonValidator(sdfRecords)
    println("\n--------------- JSON Validated Records -------------")
    jsonValidatedRecords.take(2).foreach(println)
    //jsonValidatedRecords.collect().foreach(println)

    // get the content wrapped in Success  rec_._2.get does this
    val validJsonRecords = jsonValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    //val validJsonRecords = jsonValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get.get("_p").get("data")))
    println("\n--------------- valid JSON Records ---------------")
    validJsonRecords.take(2).foreach(println)
   // validJsonRecords.collect().foreach(println)

    val invalidJsonRecords = jsonValidatedRecords.filter(_._2.isFailure)
    //writeToBigQuery(invalidJsonRecords, CMDLINEOPTIONS, JOBNAME, INVALID_JSON_ERROR)
    println("\n--------------- invalid JSON Records -------------")
    invalidJsonRecords.take(2).foreach(println)
   // invalidJsonRecords.collect().foreach(println)

    val schemaValidatedRecords = schemaValidator(validJsonRecords, schema)
    println("\n---------------- Schema Validated Records ------")
    schemaValidatedRecords.take(2).foreach(println)
  //  schemaValidatedRecords.collect.foreach(println)

    val validSchemaRecords = schemaValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    println("\n---------------- valid Schema Records ------")
    validSchemaRecords.take(2).foreach(println)
  //  validSchemaRecords.collect.foreach(println)

    val invalidSchemaRecords = schemaValidatedRecords.filter(_._2.isFailure)
    //writeToBigQuery(invalidSchemaRecords, CMDLINEOPTIONS, JOBNAME, INVALID_SCHEMA_ERROR)
    println("\n---------------- invalid Schema Records ------")
    invalidSchemaRecords.take(2).foreach(println)
//    invalidSchemaRecords.collect.foreach(println)

    println("\n---------------- retailDF with retailaSchema field types matched------")
    val retailDF = sparkSession.read.schema(Insight.retailSchema).json(validSchemaRecords.map(_._2.toString))

    retailDF.printSchema()
    retailDF.show(2,false)

    // Now view Datframe data through spark.sql
    println("\n----------------Spark sql table retail_tbl------")
    retailDF.createOrReplaceGlobalTempView("retail_tbl")
    val sparkTable  = "global_temp.retail_tbl"
    sparkSession.sql("SELECT * from global_temp.retail_tbl").show(2,false)

    println("\n----------------Revenue per stockcode------")
    sparkSession.sql("SELECT distinct stockcode,  quantity*unitprice as Revenue from global_temp.retail_tbl")
      .show(2,false)

//    println("KPI1: Highest selling SKUs on a daily basis (M,T,W,Th,F,S,Su) per country")
//    println("----------------------------------------------------")
//    Insight.runKPIQuery(sparkSession, sparkTable, CMDLINEOPTIONS.kpiLocation)



    //  BELOW code ONLY for REFERENCE

    // StructType, StructField, StringType, IntegerType, DoublleType are sql Datatypes
    // println("\n---------------- kpi1 Dataframe------")

    //val kpi1 = sparkSession.read.schema(retailSchema).json(validSchemaRecords.map(_._2.toString))
    //
    //    validSchemaRecords.show(false)
    //    val kpi1 = validSchemaRecords.map(rec => rec._2)

    //    val kp = validSchemaRecords.map(rec => rec._2)
    //    kp.
    //    val kpi1 = sparkSession.read.schema(retailSchema)
    //      .json(validSchemaRecords
    //        .map(rec => JsonTransformation.convertJsonNodesToProperFormat(rec._2).toString))


  }
}
