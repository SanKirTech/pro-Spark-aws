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

package com.sankir.smp.pipelines.validators

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.common.converters.Converter.{convertABToTryB, convertAToTryB}
import com.sankir.smp.common.validators.SchemaValidator.validateSchema
import org.apache.spark.sql.Dataset

import scala.util.Try

object Validator {

  // Try returns success or failure of JSON format
  // JsonNode is Json file - read line by line
  def jsonValidator(rawRecords: Dataset[String]): Dataset[(String, Try[JsonNode])] = {
    import com.sankir.smp.utils.encoders.CustomEncoders._
//    val str="HelloWorld"
//    convertAToTryB[String, Integer](str, (a)=> a.length)
    rawRecords.map(rec => (rec, convertAToTryB[String, JsonNode](rec, JsonUtils.toJsonNode)))
  }

  def schemaValidator(rawRecords: Dataset[(String, JsonNode)], schema: String): Dataset[(String, Try[JsonNode])] = {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    rawRecords.map(vr => (vr._1, convertABToTryB[String, JsonNode](schema, vr._2.get("_p").get("data"), validateSchema)))
    //rawRecords.map(vr => (vr._1, convertABToTryB[String, JsonNode](schema, vr._2.get("_p").get("data"), validateSchema)))  (Encoders.kryo[(String, Try[JsonNode])]) // if you remove implicit statement

  }

}
