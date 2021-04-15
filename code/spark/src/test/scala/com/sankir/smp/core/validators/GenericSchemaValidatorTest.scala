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

package com.sankir.smp.core.validators

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.JsonUtils
import com.sankir.smp.utils.FileSource
import com.sankir.smp.utils.FileSource.readAsString
import com.sankir.smp.utils.exceptions.SchemaValidationFailedException
import org.scalatest.flatspec.AnyFlatSpec

class GenericSchemaValidatorTest extends AnyFlatSpec {
  behavior of "GenericSchemaValidator"

  it should "return Failure for invalid Json" in {
    intercept[SchemaValidationFailedException] {
      val schema = FileSource.readAsString("core/validators/schema.json")
      val jsonNode: JsonNode = JsonUtils.toJsonNode(
        readAsString("core/validators/invalid_json_schema_file.json")
      )
      GenericSchemaValidator.validateSchema(schema, jsonNode)
    }
  }

  it should "return Success for valid Json" in {
    val schema = FileSource.readAsString("core/validators/schema.json")
    val jsonNode: JsonNode = JsonUtils.toJsonNode(
      readAsString("core/validators/valid_json_schema_file.json")
    )
    assert(GenericSchemaValidator.validateSchema(schema, jsonNode) == jsonNode)
  }
}
