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

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.JsonUtils
import com.sankir.smp.core.validators.DataValidator.{businessValidator, jsonValidator, schemaValidator}
import com.sankir.smp.utils.Resources.{readAsString, readAsStringIterator}
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.util.Try

/*
Unit Testing of pro-Spark
Test Units :
Data Validation -  DataValidatorTest suite
Schema Validation - GenericSchemaValidatorTest suite
Business Validation - RetailBusinessValidatorTest suite

Test Framework : ScalaTest
Testing Style : FlatSpec ( AyFlatSpec Class )
Assertions used : assert
Test data : in test/resources directory in this project

behavior, it, should and in - are all methods of class AnyFlatSpec
assert – is method in trait Assertions

 */

// Total of 6 tests in this suite
class DataValidatorTest extends AnyFlatSpec {

  implicit val stringEncoder: Encoder[String] = Encoders.STRING
  val sparkSession =
    SparkSession.builder().appName("Test").master("local[*]").getOrCreate()

  //  jsonValidator - 2 tests
  behavior of "JsonValidator"
  it should "convert valid jsons to Success objects" in {
    val rawRecords = sparkSession.createDataset(
      readAsStringIterator("core/validators/json_data_valid.json").toSeq
    )
    val jsonValidatedRecords: Dataset[(String, Try[JsonNode])] =
      jsonValidator(rawRecords)
    assert(jsonValidatedRecords.filter(_._2.isSuccess).count() == 3)
  }

  it should "convert invalid jsons to Failure objects" in {
    val rawRecords = sparkSession.createDataset(
      readAsStringIterator("core/validators/json_data_invalid.json").toSeq
    )
    val jsonValidatedRecords: Dataset[(String, Try[JsonNode])] =
      jsonValidator(rawRecords)
    assert(jsonValidatedRecords.filter(_._2.isFailure).count() == 2)
  }

  // schemaValidator - 2 tests
  behavior of "SchemaValidator"
  it should "convert valid schema to Sucess objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val rawRecords = sparkSession.createDataset(
      readAsStringIterator("core/validators/schema_data_valid.json").toSeq
    )
    val schema = readAsString("core/validators/schema.json")
    val validJsonRecords =
      rawRecords.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val schemaValidatedRecords: Dataset[(String, Try[JsonNode])] =
      schemaValidator(validJsonRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isSuccess).count() == 2)
  }

  it should "convert invalid schema to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val rawRecords = sparkSession.createDataset(
      readAsStringIterator("core/validators/schema_data_invalid.json").toSeq
    )
    val schema = readAsString("core/validators/schema.json")
    val validJsonRecords =
      rawRecords.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val schemaValidatedRecords: Dataset[(String, Try[JsonNode])] =
      schemaValidator(validJsonRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isFailure).count() == 2)
  }

  // businessValidator - 2 tests
  behavior of "BusinessValidator"
  it should "convert valid business data to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val rawRecords = sparkSession.createDataset(
      readAsStringIterator("core/validators/biz_data_valid.json").toSeq
    )
    val validSchemaRecords =
      rawRecords.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val businessValidatedRecords: Dataset[(String, Try[JsonNode])] =
      businessValidator(validSchemaRecords, RetailBusinessValidator.validate)
    assert(businessValidatedRecords.filter(_._2.isSuccess).count() == 3)
  }

  it should "convert invalid business data to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val rawRecords = sparkSession.createDataset(
      readAsStringIterator("core/validators/biz_data_invalid.json").toSeq
    )
    val validSchemaRecords =
      rawRecords.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val businessValidatedRecords: Dataset[(String, Try[JsonNode])] =
      businessValidator(validSchemaRecords, RetailBusinessValidator.validate)
    assert(businessValidatedRecords.filter(_._2.isFailure).count() == 5)
  }

}
