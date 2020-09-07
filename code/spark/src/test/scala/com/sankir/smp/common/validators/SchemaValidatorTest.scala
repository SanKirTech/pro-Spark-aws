package com.sankir.smp.common.validators

import java.io.{File, FileInputStream}

import com.fasterxml.jackson.databind.JsonNode
import com.google.gson.JsonObject
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.utils.Resources.readAsString
import com.sankir.smp.utils.exceptions.SchemaValidationFailedException
import com.sankir.smp.utils.{JsonSchema, Resources}
import org.everit.json.schema.ValidationException
import org.json.JSONObject
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConversions.asScalaBuffer
import scala.util.Failure

class SchemaValidatorTest extends AnyFlatSpec {
  behavior of "SchemaValidator"

  it should "return Failure for invalid Json" in  {
    intercept[SchemaValidationFailedException] {
      val schema = Resources.readAsString("./validators/schema.json")
      val jsonNode: JsonNode = JsonUtils.deserialize(readAsString("./validators/invalid_json_schema_file.json"))
      SchemaValidator.validateSchema(schema, jsonNode)
    }
  }

  it should "return Success for valid Json" in  {
    val schema = Resources.readAsString("./validators/schema.json")
    val jsonNode: JsonNode = JsonUtils.deserialize(readAsString("./validators/valid_json_schema_file.json"))
    val result = SchemaValidator.validateSchema(schema, jsonNode)
    assert(SchemaValidator.validateSchema(schema, jsonNode) == jsonNode)
  }
}
