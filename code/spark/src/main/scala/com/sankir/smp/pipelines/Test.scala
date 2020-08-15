package com.sankir.smp.pipelines

import java.io.{BufferedReader, ByteArrayInputStream, File, FileInputStream, InputStream}
import java.util.stream.Collectors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.api.services.bigquery.model.TableRow
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery.{BigQueryOptions, InsertAllRequest, TableId}
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.app.JsonUtils.getObjectProperty
import com.sankir.smp.connectors.BigQueryIO
import com.sankir.smp.utils.JsonSchema
import org.apache.spark.sql.internal.SQLConf.HiveCaseSensitiveInferenceMode
import org.everit.json.schema.ValidationException
import org.json.JSONObject


object Test {
  def main(args: Array[String]): Unit = {
    import org.everit.json.schema.loader.SchemaLoader

    val schemaFile: InputStream = new FileInputStream("./code/spark/schema2.json");
    val schema = JsonSchema.fromJsonFile(schemaFile)


    val jsonString =
      """
        |{
        |    "productId": 1,
        |    "productName": "An ice sculpture",
        |    "price": 12.50,
        |    "tags": [ "cold", "ice" ],
        |    "dimensions": {
        |      "length": 7.0,
        |      "width": 12.0,
        |      "height": 9.5
        |    },
        |    "warehouseLocationey": "asdfasdfasd"
        |  }""".stripMargin

    val testData: JsonNode = JsonUtils.deserialize(jsonString)


    val jsonString2 = "{\"_m\": {\"_rt\": \"2020-07-03 20:19:07\", \"_src\": \"gcs\", \"_o\": \"\", \"src_dtls\": \"https://storage.googleapis.com/sankir-1705/retail/input/2010-12-01.csv\"}, \"_p\": {\"data\": {\"InvoiceNo\": \"536365-Kiran\", \"StockCode\": \"85123A\", \"Description\": \"WHITE HANGING HEART T-LIGHT HOLDER\", \"Quantity\": \"6\", \"InvoiceDate\": \"2010-12-01 08:26:00\", \"UnitPrice\": \"2.55\", \"CustomerID\": \"17850.0\", \"Country\": \"United Kingdom\"}}}"

    val data = JsonUtils.deserialize(jsonString2)
    println(data)
    val newNode = JsonUtils.emptyObject()
    val metadata = JsonUtils.getObjectProperty(data,"_m")
    metadata.remove("src_dtls")
    newNode.setAll(metadata)
    val payload = JsonUtils.getObjectProperty(data,"_p")
    newNode.setAll(JsonUtils.getObjectProperty(payload,"data"))
    println(newNode)

    val row = JsonUtils.MAPPER.convertValue(newNode, classOf[TableRow])
    val bigQueryIO = BigQueryIO(projectId = "sankir-1705")
    bigQueryIO.insertRow("retail_bq", "t_transaction", row)








//    val tableId = TableId.of("sankir-1705","retail_bq", "t_transaction")
//
    val credentialsPath = new File("F:\\extra-work\\lockdown_usecases\\SparkUsecase\\key.json")
    val googleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(credentialsPath))
//    val BigQuery = BigQueryOptions.newBuilder()
//      .setCredentials(googleCredentials)
//      .setProjectId("sankir-1705")
//      .build()
//      .getService
//    val response = BigQuery.insertAll(InsertAllRequest.newBuilder(tableId).addRow(row).build())
//    println(row)
//    println(response)

//    val bigQueryIO = BigQueryIO(googleCredentials= googleCredentials,projectId = "sankir-1705")


    schema


//    println(scala.io.Source.fromInputStream(schema).mkString)
//    val jN:JsonNode = JsonUtils.deserialize(scala.io.Source.fromInputStream(schemaFromFile).mkString)
//    println(jN)
//    println(new JSONObject(jN))



//    val schema = JsonSchema.fromJsonNode(jN)



//    val x = JsonUtils.emptyObject().put("productId", "asdadad")


//    println(testData)

//    try {
////      val example = new JSONObject(testData.toString)
//      schema.validate(example)
//      println("ZZZZZZZZZZZZZZZZZ")
//      println(example)
//    } catch {
//      case ex: ValidationException =>
//        println("-------------")
//        println(ex.getErrorMessage)
//        println(ex.getMessage)
//        println(ex.getSchemaLocation)
//        println(ex.getPointerToViolation)
//        println(ex.getViolatedSchema)
//        println(ex.getKeyword)
//        println(ex.getLocalizedMessage)
//        ex.getAllMessages.toArray().foreach(println)
//    }

  }

}
