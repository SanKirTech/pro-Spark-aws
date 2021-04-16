/*
 *
 *  * SanKir Technologies
 *  * (c) Copyright 2021.  All rights reserved.
 *  * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  *  without the prior written permission from Sankir Technologies.
 *  *
 *  * The course contents can be accessed by subscribing to pro-Spark course.
 *  *
 *  * Please visit www.sankir.com for details.
 *  *
 *
 */

package com.sankir.smp.core.validators

import com.sankir.smp.core.SharedSparkContext
import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.JsonUtils
import com.sankir.smp.core.validators.DataValidator.{businessValidator, jsonValidator, schemaValidator}
import com.sankir.smp.utils.FileSource.{readAsString, readAsStringIterator}
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row}

import scala.util.Try

class DataValidatorTest extends AnyFlatSpec with SharedSparkContext{

  implicit val stringEncoder: Encoder[String] = Encoders.STRING
  behavior of "JsonValidator"
  it should "convert invalid jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/validators/invalid_data.json").toSeq
    )
    val jsonValidatedRecords = jsonValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isFailure).count() == 4)
  }

  it should "convert valid jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/validators/valid_data.json").toSeq
    )
    val jsonValidatedRecords = jsonValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isSuccess).count() == 5)
  }

  behavior of "SchemaValidator"
  it should "convert invalid schema jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/validators/invalid_schema_data.json").toSeq
    )
    val schema = readAsString("core/validators/schema.json")
    val jsonValidatedRecords =
      sdfData.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val schemaValidatedRecords = schemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isFailure).count() == 1)
  }

  it should "convert valid schema jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/validators/valid_schema_data_with_payload.json").toSeq
    )
    val schema = readAsString("core/validators/schema.json")
    val jsonValidatedRecords =
      sdfData.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val schemaValidatedRecords = schemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isSuccess).count() == 1)
  }

  behavior of "BusinessValidator"
  it should "convert invalid biz data to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/validators/invalid_biz_data.json").toSeq
    )
    val validSchemaRecords =
      sdfData.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val businessValidatedRecords: Dataset[(String, Try[JsonNode])] =
      businessValidator(validSchemaRecords, RetailBusinessValidator.validate)
    assert(businessValidatedRecords.filter(_._2.isFailure).count() == 5)
  }

  it should "convert valid biz data to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/validators/valid_biz_data.json").toSeq
    )
    val validSchemaRecords =
      sdfData.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val businessValidatedRecords: Dataset[(String, Try[JsonNode])] =
      businessValidator(validSchemaRecords, RetailBusinessValidator.validate)
    assert(businessValidatedRecords.filter(_._2.isSuccess).count() == 5)
  }
}
