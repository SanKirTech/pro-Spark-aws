package com.sankir.smp.pipelines

import com.sankir.smp.pipelines.transformations.ErrorTransformations.writeToBigQuery
import com.sankir.smp.pipelines.validators.Validator.{jsonSchemaValidator, jsonStringValidator}
import com.sankir.smp.utils.ArgParser
import com.sankir.smp.utils.Resources.readAsStringFromGCS
import com.sankir.smp.utils.enums.ErrorEnums.{INVALID_JSON_ERROR, SCHEMA_VALIDATION_ERROR}
import org.apache.spark.sql.SparkSession

object ApplicationMain {
  def main(args: Array[String]): Unit = {

    //    Parsing the Arguments and updating the config object that will help us in providing necessary inputs
    val CMDLINEOPTIONS = ArgParser.parse(args)

    //Reading the schemaString
    val schema = readAsStringFromGCS(CMDLINEOPTIONS.projectId, CMDLINEOPTIONS.schemaLocation)

    // Creating SparkSession
    val sparkSession = SparkSession.builder().appName("Pro-Spark-Batch").getOrCreate()

    val JOBNAME = s"${sparkSession.sparkContext.appName}-${sparkSession.sparkContext.applicationId}"

    // Reading data from the input location
    import com.sankir.smp.utils.encoders.CustomEncoders._

    // sdfData is the dataset crated from the processedJSON with metadata
    val sdfData = sparkSession.read.textFile(CMDLINEOPTIONS.inputLocation)

    // jsonStringValidator validates whether the input data is valid or not
    val jsonValidatedRecords = jsonStringValidator(sdfData)
    val jsonRecords = jsonValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    val inValidJsonRecords = jsonValidatedRecords.filter(_._2.isFailure)
    writeToBigQuery(inValidJsonRecords, CMDLINEOPTIONS, JOBNAME, INVALID_JSON_ERROR)

    val schemaValidatedRecords = jsonSchemaValidator(jsonRecords, schema)
    val jsonRecordsWithProperSchema = schemaValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2))
    val invalidSchemaRecords = schemaValidatedRecords.filter(_._2.isFailure)
    writeToBigQuery(invalidSchemaRecords, CMDLINEOPTIONS, JOBNAME, SCHEMA_VALIDATION_ERROR)

  }
}
