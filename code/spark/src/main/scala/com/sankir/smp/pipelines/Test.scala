package com.sankir.smp.pipelines

import java.io.{BufferedReader, ByteArrayInputStream, FileInputStream, InputStream}
import java.util.stream.Collectors

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.app.JsonUtils
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
