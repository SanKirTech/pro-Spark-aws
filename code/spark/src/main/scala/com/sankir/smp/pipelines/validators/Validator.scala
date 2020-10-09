package com.sankir.smp.pipelines.validators

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.common.converters.Converter.{convertABToTryB, convertAToTryB}
import com.sankir.smp.common.validators.SchemaValidator.validateSchema
import org.apache.spark.sql.Dataset

import scala.util.Try

object Validator {

  // Try returns success or failure of JSON format
  // JsonNode is Json file - read line by line
  def jsonValidator(rawRecords: Dataset[String]): Dataset[(String, Try[JsonNode])] = {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    rawRecords.map(rec => (rec, convertAToTryB[String, JsonNode](rec, JsonUtils.deserialize)))
  }

  def schemaValidator(rawRecords: Dataset[(String, JsonNode)], schema: String): Dataset[(String, Try[JsonNode])] = {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    rawRecords.map(vr => (vr._1, convertABToTryB[String, JsonNode](schema, vr._2.get("_p").get("data"), validateSchema)))
  }


}
