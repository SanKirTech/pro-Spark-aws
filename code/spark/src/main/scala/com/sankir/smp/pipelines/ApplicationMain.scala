package com.sankir.smp.pipelines

import com.fasterxml.jackson.databind.JsonNode
import com.google.api.services.bigquery.model.TableRow
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.common.converters.Converter._
import com.sankir.smp.common.validators.SchemaValidator
import com.sankir.smp.connectors.{BigQueryIO, GcsIO}
import com.sankir.smp.utils.Resources.readAsString
import com.sankir.smp.utils.enums.ErrorEnums
import com.sankir.smp.utils.{Config, JsonSchema}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.everit.json.schema.Schema

import scala.util.Try


object ApplicationMain {

  def main(args: Array[String]): Unit = {


    //    val CONFIG = ArgParser.parse(args);
    val CONFIG = Config(
      projectId = "sankir-1705",
      inputLocation = "F:\\extra-work\\lockdown_usecases\\SparkUsecase\\code\\spark\\input.json",
//      schemaLocation = "F:\\extra-work\\lockdown_usecases\\SparkUsecase\\infrastructure\\terraforms\\project\\json-schema\\t_transaction.json"
      schemaLocation = "./t_transaction.json"
    )

    val gcsIO = GcsIO(projectId = CONFIG.projectId)
//    val schema = JsonSchema.fromJson(gcsIO.getData(CONFIG.schemaLocation))
    val schemaString = readAsString(CONFIG.schemaLocation)


    val sparkSession = SparkSession.builder().appName("Pro-Spark-Batch").master("local[*]").getOrCreate();
    val jobName = s"${sparkSession.sparkContext.appName}-${sparkSession.sparkContext.applicationId}"
    val rawData = sparkSession.read.textFile(CONFIG.inputLocation)
    implicit val jsonNodeEncoder = Encoders.kryo[(String, Try[JsonNode])]
    implicit val tableRowEncoder = Encoders.kryo[TableRow]
    val jsonRecords = rawData.map(rec => convertAToTryTuple[String, JsonNode](rec, JsonUtils.deserialize(_)))
    jsonRecords.cache()
    val validJsonRecords = jsonRecords.filter(_._2.isSuccess)
    val inValidJsonRecords = jsonRecords.filter(_._2.isFailure)

//    inValidJsonRecords.map(errMsg =>
//      convertToErrorTableRows[JsonNode](errMsg, ErrorEnums.INVALID_JSON_ERROR, jobName))
//      .foreachPartition(tableRows => {
//        val bigQueryIO = BigQueryIO(projectId = CONFIG.projectId)
//        bigQueryIO.insertIterableRows("retail_bq", "t_error", tableRows.toIterable)
//      })

    validJsonRecords.map(vr => convertABToTryTuple[String, JsonNode, String](schemaString, vr._2.get.get("_p").get("data"), SchemaValidator.validateJson(_, _), vr._1))
      .filter(_._2.isFailure)
      .collect().foreach(println)










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
