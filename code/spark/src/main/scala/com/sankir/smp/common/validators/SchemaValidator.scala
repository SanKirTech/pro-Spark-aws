package com.sankir.smp.common.validators

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.utils.JsonSchema
import com.sankir.smp.utils.exceptions.SchemaValidationFailedException
import org.everit.json.schema.{Schema, ValidationException}
import org.json.JSONObject

import scala.collection.JavaConversions.asScalaBuffer
import scala.util.{Failure, Success, Try}
;

object SchemaValidator {
    def validateSchema(schemaString: String, jsonNode:JsonNode) : JsonNode = {
      try {
        val schema = JsonSchema.fromJson(schemaString)
        schema.validate(new JSONObject(jsonNode.toString))
        jsonNode
      } catch {
        case validationException: ValidationException => {
          throw SchemaValidationFailedException(validationException.getAllMessages.mkString("\n"))
        }
      }
    }
}
