package com.sankir.smp.common.converters

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.app.JsonUtils

import scala.util.Try

object Converter {

  def convertToJsonNode(jsonString: String): Tuple2[String, Try[JsonNode]] =
    (jsonString, Try(JsonUtils.deserialize(jsonString)))

}