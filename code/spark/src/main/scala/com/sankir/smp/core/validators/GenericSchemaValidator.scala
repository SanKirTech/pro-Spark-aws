/*
 *
 *  * SanKir Technologies
 *  * (c) Copyright 2021.  All rights reserved.
 *  * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  *  without the prior written permission from Sankir Technologies.
 *  *
 *  * The course contents can be accessed by subscribing to pro-Spark course.
 *  *
 *  * Please visit www.sankir.com for details.
 *  *
 *
 */

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

  /***
    * validateSchema
    * @param schemaString - json schema
    * @param jsonNode  - JsonNode object as defined by Jackson library
    * @return returns a JsonNod
    */
  def validateSchema(schemaString: String, jsonNode: JsonNode): JsonNode = {
    try {
      val schema = JsonSchema.fromJson(schemaString)
      schema.validate(new JSONObject(jsonNode.toString))
      jsonNode
    } catch {
      case validationException: ValidationException =>
        throw SchemaValidationFailedException(
          validationException.getAllMessages.mkString("\n")
        )
    }
  }
}
