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

package com.sankir.smp.utils

import java.io.InputStream

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.JsonUtils
import org.everit.json.schema.Schema
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONObject, JSONTokener}

object JsonSchema {

  /***
    *
    * @param jsonString takes in json string
    * @return returns a schema object
    */
  def fromJson(jsonString: String): Schema =
    loadSchema(new JSONTokener(jsonString))

  /***
    *
    * @param jsonTokener
    * @return
    */
  private def loadSchema(jsonTokener: JSONTokener): Schema = {
    val loader: SchemaLoader = SchemaLoader
      .builder()
      .useDefaults(true)
      .schemaJson(new JSONObject(jsonTokener))
      .draftV7Support()
      .build()
    val schema: Schema = loader.load().build().asInstanceOf[Schema]
    schema
  }

  /***
    *
    * @param jsonSchema
    * @return
    */
  def fromJsonFile(jsonSchema: InputStream): Schema =
    loadSchema(new JSONTokener(jsonSchema))

  /***
    *
    * @param jsonSchema
    * @return
    */
  def fromJsonNode(jsonSchema: JsonNode): Schema = {
    loadSchema(new JSONTokener(JsonUtils.jsonNodetoString(jsonSchema)))
  }

}
