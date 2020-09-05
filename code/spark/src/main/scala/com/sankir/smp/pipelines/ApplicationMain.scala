package com.sankir.smp.pipelines

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.google.api.services.bigquery.model.TableRow
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider
import com.jayway.jsonpath.{Configuration, JsonPath}
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.common.converters.Converter._
import com.sankir.smp.connectors.{BigQueryIO, GcsIO, PubSubIO}
import com.sankir.smp.utils.{ArgParser, JsonSchema}
import org.apache.spark.sql.{Encoders, SparkSession}

import scala.util.Try


object ApplicationMain {

  def main(args: Array[String]): Unit = {


    val CONFIG = ArgParser.parse(args);

    val gcsIO = GcsIO(projectId = CONFIG.projectId)
    val schema = JsonSchema.fromJson(gcsIO.getData(CONFIG.schemaLocation))

    val sparkSession = SparkSession.builder().appName("Pro-Spark-Batch").master("local[*]").getOrCreate();
    val rawData = sparkSession.read.textFile(CONFIG.inputLocation)
    implicit val jsonNodeEncoder = Encoders.kryo[(String, Try[JsonNode])]
    implicit val tableRowEncoder = Encoders.kryo[TableRow]
    val jsonRecords = rawData.map(convertToJsonNodeTuple(_))
    jsonRecords.cache()
    val validJsonRecords = jsonRecords.filter(_._2.isSuccess)
    val inValidJsonRecords = jsonRecords.filter(_._2.isFailure)

    inValidJsonRecords.map(errMsg =>
      convertToErrorTableRows[JsonNode](errMsg, sparkSession.sparkContext.applicationId))
      .foreachPartition( tableRows => {
        val bigQueryIO = BigQueryIO(projectId = "sankir-1705")
        bigQueryIO.insertIterableRows("retail_bq", "t_transaction", tableRows.toIterable)
      })


    validJsonRecords.collect().foreach(println)







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
