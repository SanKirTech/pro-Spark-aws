package com.sankir.smp.pipelines.validators


import com.fasterxml.jackson.databind.JsonNode

import scala.util.Try

trait BusinessValidator {
  def validate(data: JsonNode): Try[JsonNode]
}
