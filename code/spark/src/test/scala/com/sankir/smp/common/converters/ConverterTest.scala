package com.sankir.smp.common.converters

import org.scalatest.flatspec.AnyFlatSpec

class ConverterTest extends AnyFlatSpec {

  behavior of "convertToJsonNode"

  it should "return success when converting jsonString" in {
    val jsonString = "{\"key\": \"value\"}"
    val result = Converter.convertToJsonNodeTuple(jsonString)
    assert(result._2.isSuccess)
  }

  it should "return failure when converting a bad json string" in {
    val jsonString = "{\"key\": \"value\""
    val result = Converter.convertToJsonNodeTuple(jsonString)
    assert(result._2.isFailure)
  }


}
