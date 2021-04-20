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
import com.sankir.smp.core.validators.DataValidator.{
  businessValidator,
  jsonValidator,
  schemaValidator
}
import com.sankir.smp.utils.FileSource.{readAsString, readAsStringIterator}
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row}

import scala.util.Try

class DataValidatorTest extends AnyFlatSpec with SharedSparkContext {

  implicit val stringEncoder: Encoder[String] = Encoders.STRING

  //  jsonValidator

  behavior of "JsonValidator"
  it should "convert valid jsons to Success objects and invalid jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val rawRecords = sparkSession.createDataset(
      readAsStringIterator("core/validators/json_data.json").toSeq
    )
    val jsonValidatedRecords = jsonValidator(rawRecords)
    assert(jsonValidatedRecords.filter(_._2.isSuccess).count() == 3)
    assert(jsonValidatedRecords.filter(_._2.isFailure).count() == 2)

  }

  // schemaValidator

  behavior of "SchemaValidator"
  it should "convert valid schema jsons to Failure objects and invalid schema jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val rawRecords = sparkSession.createDataset(
      readAsStringIterator("core/validators/schema_data.json").toSeq
    )
    val schema = readAsString("core/validators/schema.json")
    val validJsonRecords =
      rawRecords.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val schemaValidatedRecords = schemaValidator(validJsonRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isSuccess).count() == 2)
    assert(schemaValidatedRecords.filter(_._2.isFailure).count() == 1)

  }

  // businessValidator

  behavior of "BusinessValidator"
  it should "convert valid biz data to Success objects and convert invalid biz data to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val rawRecords = sparkSession.createDataset(
      readAsStringIterator("core/validators/biz_data.json").toSeq
    )
    val validSchemaRecords =
      rawRecords.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val businessValidatedRecords: Dataset[(String, Try[JsonNode])] =
      businessValidator(validSchemaRecords, RetailBusinessValidator.validate)
    assert(businessValidatedRecords.filter(_._2.isSuccess).count() == 3)
    assert(businessValidatedRecords.filter(_._2.isFailure).count() == 5)

  }

}
