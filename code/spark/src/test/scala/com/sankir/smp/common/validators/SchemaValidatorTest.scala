/*
 * SanKir Technologies
 * (c) Copyright 2020.  All rights reserved.
 * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  without the prior written permission from Sankir Technologies.
 *
 * The course contents can be accessed by subscribing to pro-Spark course.
 *
 * Please visit www.sankir.com for details.
 *
 */

package com.sankir.smp.common.validators

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.utils.Resources
import com.sankir.smp.utils.Resources.readAsString
import com.sankir.smp.utils.exceptions.SchemaValidationFailedException
import org.scalatest.flatspec.AnyFlatSpec

class SchemaValidatorTest extends AnyFlatSpec {
  behavior of "SchemaValidator"

  it should "return Failure for invalid Json" in  {
    intercept[SchemaValidationFailedException] {
      val schema = Resources.readAsString("./validators/schema.json")
      val jsonNode: JsonNode = JsonUtils.toJsonNode(readAsString("./validators/invalid_json_schema_file.json"))
      SchemaValidator.validateSchema(schema, jsonNode)
    }
  }

  it should "return Success for valid Json" in  {
    val schema = Resources.readAsString("./validators/schema.json")
    val jsonNode: JsonNode = JsonUtils.toJsonNode(readAsString("./validators/valid_json_schema_file.json"))
    assert(SchemaValidator.validateSchema(schema, jsonNode) == jsonNode)
  }
}
