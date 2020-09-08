package com.sankir.smp.app

import com.fasterxml.jackson.core.JsonParseException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class JsonUtilsTest extends AnyFlatSpec {

  behavior of "JsonUtils"

  it should "be able to parse a json string" in {
    val jsonNode = JsonUtils.deserialize("{\"key\":\"value\"}")
    assert(jsonNode.get("key").asText() == "value")
  }

  it should "throw JsonParseException when incorrect json string is parsed" in {
    intercept[JsonParseException] {
      JsonUtils.deserialize("{\"key\":\"value\"")
    }
  }

  it should "be able to parse bytes" in {
    val jsonNode = JsonUtils.deserialize("{\"key\":\"value\"}".getBytes)
    assert(jsonNode.get("key").asText() == "value")
  }

  it should "return option when asStringPropertyOptional called" in {
    val jsonNode = JsonUtils.emptyObject().put("key", "value")
    JsonUtils.asStringPropertyOptional(jsonNode, "key") shouldEqual Some("value")
  }

  it should "return option when getLongPropertyOptional called" in {
    val jsonNode = JsonUtils.emptyObject().put("key", 10L)
    JsonUtils.getLongPropertyOptional(jsonNode, "key") shouldEqual Some(10)
  }

  it should "return long if the value is a string" in {
    val jsonNode = JsonUtils.emptyObject().put("key", "10")
    val result = JsonUtils.asLongPropertyOptional(jsonNode, "key")
    assert(result.contains(10))
  }

}
