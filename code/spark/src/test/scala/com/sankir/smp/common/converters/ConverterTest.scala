package com.sankir.smp.common.converters

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.app.JsonUtils
import io.grpc.internal.JsonUtil
import org.scalatest.flatspec.AnyFlatSpec

class ConverterTest extends AnyFlatSpec {

  behavior of "convertToJsonNode"

  it should "return success when converting jsonString" in {
    val jsonString = "{\"key\": \"value\"}"
    val result = Converter.convertAToTryB[String,JsonNode](jsonString, JsonUtils.deserialize(_))
    assert(result.isSuccess)
  }

  it should "return failure when converting a bad json string" in {
    val jsonString = "{\"key\": \"value\""
    val result = Converter.convertAToTryB[String,JsonNode](jsonString, JsonUtils.deserialize(_))
    assert(result.isFailure)
  }


}
