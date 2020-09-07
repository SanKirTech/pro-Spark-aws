package com.sankir.smp.utils.encoders

import com.fasterxml.jackson.databind.JsonNode
import com.google.api.services.bigquery.model.TableRow
import org.apache.spark.sql.{Encoder, Encoders}

import scala.util.Try

object CustomEncoders {
  implicit val stringJsonNodeEncoder: Encoder[(String,JsonNode)] = Encoders.kryo[(String,JsonNode)]
  implicit val jsonNodeTupleEncoder: Encoder[(String, Try[JsonNode])] = Encoders.kryo[(String, Try[JsonNode])]
  implicit val tableRowEncoder: Encoder[TableRow] = Encoders.kryo[TableRow]
}
