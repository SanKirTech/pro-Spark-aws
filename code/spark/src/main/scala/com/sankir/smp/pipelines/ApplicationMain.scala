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
    val CONFIG = ArgParser.parse(args)

    //Reading the schemaString
    val schema = readAsStringFromGCS(CONFIG.projectId, CONFIG.schemaLocation)


    // Creating SparkSession
    val sparkSession = SparkSession.builder().appName("Pro-Spark-Batch").getOrCreate()

    val JOBNAME = s"${sparkSession.sparkContext.appName}-${sparkSession.sparkContext.applicationId}"

    // Reading data from the input location
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val rawData = sparkSession.read.textFile(CONFIG.inputLocation)


    val jsonValidatedRecords = jsonStringValidator(rawData)
    val jsonRecords = jsonValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    val inValidJsonRecords = jsonValidatedRecords.filter(_._2.isFailure)
    writeToBigQuery(inValidJsonRecords, CONFIG, JOBNAME, INVALID_JSON_ERROR)

    val schemaValidatedRecords = jsonSchemaValidator(jsonRecords, schema)
    val jsonRecordsWithProperSchema = schemaValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2))
    val invalidSchemaRecords = schemaValidatedRecords.filter(_._2.isFailure)
    writeToBigQuery(invalidSchemaRecords, CONFIG, JOBNAME, SCHEMA_VALIDATION_ERROR)


    //TODO: Read data from GCS Location into a Dataset
    //TODO: Convert json String to SDF object [ Data Validation]
    //TODO: Rule Validation
    //TODO: Store correct data in Big Query
    //TODO: Store error data in Big Query
    //TODO: Send data to pubSub


    //    val inputPath = args(0)
    //    val outputPath = args(1)
    //
    //    val sc = new SparkContext(new SparkConf().setAppName("Word Count"))
    //    val lines = sc.textFile(inputPath)
    //    val words = lines.flatMap(line => line.split(" "))
    //    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    //    wordCounts.saveAsTextFile(outputPath)

    //    publisherExample("sankir-1705", "projects/sankir-1705/topics/sample")
//        val pubSubIO = PubSubIO("sankir-1705","sample")
//        pubSubIO.publishMessage("hello world")

//    val jsonString = "{\"error\": \"schema-validation-error\", \"message\": \"not a valid string\"}"
//
//    val errorMessageInJson: JsonNode =
//      JsonUtils.emptyObject().set("error", JsonUtils.emptyObject().put("message", "schema-validation-error"))
//
//    val errorMessage2: ArrayNode = JsonUtils.emptyArray().add(JsonUtils.emptyObject().put("test", "result"))
//    println(errorMessage2)
//
//    val jsonPath = JsonPath.compile("$.error.message")
//
//    val jsonPathConfig =
//      Configuration.builder()
//        .jsonProvider(new JacksonJsonNodeJsonProvider(JsonUtils.MAPPER))
//        .mappingProvider(new JacksonMappingProvider(JsonUtils.MAPPER))
//        .options(com.jayway.jsonpath.Option.ALWAYS_RETURN_LIST)
//        .build()
//
//    val result = JsonPath.parse(errorMessageInJson, jsonPathConfig)
//    //    JsonPath.compile()
//    val x: ArrayNode = JsonPath.parse(errorMessageInJson, jsonPathConfig).read(jsonPath)
//    println(x)


    //    JsonPath.parse(errorMessageInJson,jsonPath)
    //
    //    println(errorMessageInJson.path("$.error.message"))


    //    pubSubIO.publishMessage(errorMessageInJson.asInstanceOf[ObjectNode])
    //    pubSubIO.close()





  }




}
