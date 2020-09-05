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
    def validateJson(schemaString: String, jsonNode:JsonNode) : Try[JsonNode] = {
      try {
        val schema = JsonSchema.fromJson(schemaString)
        schema.validate(new JSONObject(jsonNode.toString))
        Success(jsonNode)
      } catch {
        case validationException: ValidationException => {
          Failure(SchemaValidationFailedException(validationException.getAllMessages.mkString("\n")))
        }
      }
    }
}
