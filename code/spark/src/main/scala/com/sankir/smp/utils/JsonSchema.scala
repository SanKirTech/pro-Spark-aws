package com.sankir.smp.utils

import java.io.InputStream

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.app.JsonUtils
import org.everit.json.schema.Schema
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONObject, JSONTokener}

object JsonSchema {

  def fromJson(jsonString: String) =
    loadSchema(new JSONTokener(jsonString))

  private def loadSchema(jsonTokener: JSONTokener): Schema = {
    val loader: SchemaLoader = SchemaLoader.builder()
      .useDefaults(true)
      .schemaJson(new JSONObject(jsonTokener))
      .draftV7Support()
      .build()
    val schema: Schema = loader.load().build()
    schema
  }

  def fromJsonFile(jsonSchema: InputStream) =
    loadSchema(new JSONTokener(jsonSchema))

  def fromJsonNode(jsonSchema: JsonNode) : Schema = {
    loadSchema(new JSONTokener(JsonUtils.serialize(jsonSchema)))
  }

}
