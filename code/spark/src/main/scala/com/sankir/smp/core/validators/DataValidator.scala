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

package com.sankir.smp.core.validators

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.JsonUtils
import com.sankir.smp.common.Converter.{convertABToTryB, convertAToTryB}
import com.sankir.smp.core.validators.GenericSchemaValidator.validateSchema
import org.apache.spark.sql.Dataset

import scala.util.Try

object DataValidator {

  // Try returns success or failure of JSON format
  // JsonNode is Json file - read line by line
  def jsonValidator(rawRecords: Dataset[String]): Dataset[(String, Try[JsonNode])] = {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    rawRecords.map(rec => (rec, convertAToTryB[String, JsonNode](rec, JsonUtils.toJsonNode)))
  }

  def schemaValidator(rawRecords: Dataset[(String, JsonNode)], schema: String): Dataset[(String, Try[JsonNode])] = {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    rawRecords.map(vr => (vr._1, convertABToTryB[String, JsonNode](schema, vr._2.get("_p").get("data"), validateSchema)))
    //rawRecords.map(vr => (vr._1, convertABToTryB[String, JsonNode](schema, vr._2.get("_p").get("data"), validateSchema)))  (Encoders.kryo[(String, Try[JsonNode])]) // if you remove implicit statement
  }

  def businessValidator(rawRecords: Dataset[(String, JsonNode)], fun: JsonNode => Try[JsonNode]): Dataset[(String, Try[JsonNode])] = {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    rawRecords.map(rec => (rec._1, fun.apply(rec._2)))
    //rawRecords.map(vr => (vr._1, convertABToTryB[String, JsonNode](schema, vr._2.get("_p").get("data"), validateSchema)))  (Encoders.kryo[(String, Try[JsonNode])]) // if you remove implicit statement

  }

}