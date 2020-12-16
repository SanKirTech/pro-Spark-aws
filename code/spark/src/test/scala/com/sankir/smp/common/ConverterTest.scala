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

package com.sankir.smp.common

import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.flatspec.AnyFlatSpec

class ConverterTest extends AnyFlatSpec {

  behavior of "convertToJsonNode"

  it should "return success when converting jsonString" in {
    val jsonString = "{\"key\": \"value\"}"
    val result = Converter
      .convertAToTryB[String, JsonNode](jsonString, JsonUtils.toJsonNode)
    assert(result.isSuccess)
  }

  it should "return failure when converting a bad json string" in {
    val jsonString = "{\"key\": \"value\""
    val result = Converter
      .convertAToTryB[String, JsonNode](jsonString, JsonUtils.toJsonNode)
    assert(result.isFailure)
  }

}
