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
import com.sankir.smp.gcp.GCPConnector
import com.sankir.smp.gcp.GCPConnector.readAsString
import com.sankir.smp.utils.exceptions.SchemaValidationFailedException
import org.scalatest.flatspec.AnyFlatSpec

// Total of 2 tests in this suite
class GenericSchemaValidatorTest extends AnyFlatSpec {

  //  GenericSchemaValidator - 2 tests
  behavior of "GenericSchemaValidator"
  it should "return Success for valid Schema" in {
    val schema = GCPConnector.readAsString("core/validators/schema.json")
    val jsonNode: JsonNode =
      JsonUtils.toJsonNode(readAsString("core/validators/schema_valid.json"))
    assert(
      GenericSchemaValidator.validateSchema(schema, jsonNode).equals(jsonNode)
    )
  }

  it should "return Failure for invalid Schema" in {
    val schema = GCPConnector.readAsString("core/validators/schema.json")
    val jsonNode: JsonNode =
      JsonUtils.toJsonNode(readAsString("core/validators/schema_invalid.json"))
    assertThrows[SchemaValidationFailedException](
      GenericSchemaValidator.validateSchema(schema, jsonNode)
    )
  }

}
