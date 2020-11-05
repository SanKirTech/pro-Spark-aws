/*
 * Comment for pipeline module
 * Scala objects
 * Spark sql table
 * KPI tables
 *
 */

package com.sankir.smp.core.validators

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.utils.JsonSchema
import com.sankir.smp.utils.exceptions.SchemaValidationFailedException
import org.everit.json.schema.ValidationException
import org.json.JSONObject

import scala.collection.JavaConversions.asScalaBuffer

// To Validate JsonNode when Schema is provided
object GenericSchemaValidator {
  def validateSchema(schemaString: String, jsonNode: JsonNode): JsonNode = {
    try {
      val schema = JsonSchema.fromJson(schemaString)
      schema.validate(new JSONObject(jsonNode.toString))
      jsonNode
    } catch {
      case validationException: ValidationException =>
        throw SchemaValidationFailedException(validationException.getAllMessages.mkString("\n"))
    }
  }
}
