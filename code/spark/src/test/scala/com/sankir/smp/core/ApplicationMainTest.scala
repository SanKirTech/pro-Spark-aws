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

package com.sankir.smp.core

import com.sankir.smp.common.JsonUtils
import com.sankir.smp.core.validators.DataValidator.{
  jsonValidator,
  schemaValidator
}
import com.sankir.smp.utils.FileSource.{readAsString, readAsStringIterator}
import org.scalatest.flatspec.AnyFlatSpec

class ApplicationMainTest extends AnyFlatSpec with SharedSparkContext {

  behavior of "Application"

  it should "should convert invalid jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("pipelines/invalid_json_data.txt").toSeq
    )
    val jsonValidatedRecords = jsonValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isFailure).count() == 4)
  }

  it should "should convert valid jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("pipelines/valid_json_data.txt").toSeq
    )
    val jsonValidatedRecords = jsonValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isSuccess).count() == 3)
  }

  it should "should convert invalid schema jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("pipelines/schema_json_data.txt").toSeq
    )
    val schema = readAsString("pipelines/schema.json")
    val jsonValidatedRecords =
      sdfData.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val schemaValidatedRecords = schemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isFailure).count() == 1)
  }

  it should "should convert valid schema jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("pipelines/schema_json_data.txt").toSeq
    )
    val schema = readAsString("pipelines/schema.json")
    val jsonValidatedRecords =
      sdfData.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val schemaValidatedRecords = schemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isSuccess).count() == 2)
  }

  it should "Actual Data" ignore {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("pipelines/SixRecs.json").toSeq
    )

    println("\n--------  sdfData ------------")
    sdfData.show(20, false)

    val schema = readAsString("pipelines/t_transaction_trimmed.json")

    val jsonValidatedRecords = jsonValidator(sdfData)
    val jsonRecords = jsonValidatedRecords
      .filter(_._2.isSuccess)
      .map(rec => (rec._1, rec._2.get))
    val inValidJsonRecords = jsonValidatedRecords.filter(_._2.isFailure)
    // writeToBigQuery(inValidJsonRecords, CMDLINEOPTIONS, JOBNAME, INVALID_JSON_ERROR)
    println("\n--------------- invalid JSON records -------------")
    inValidJsonRecords.collect().foreach(println)

    println("\n--------------- valid JSON records ---------------")
    jsonRecords.collect().foreach(println)

    val schemaValidatedRecords = schemaValidator(jsonRecords, schema)
    val jsonRecordsWithProperSchema =
      schemaValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2))
    val invalidSchemaRecords = schemaValidatedRecords.filter(_._2.isFailure)
    //writeToBigQuery(invalidSchemaRecords, CMDLINEOPTIONS, JOBNAME, INVALID_SCHEMA_ERROR)

    println("\n---------------- invalid Schema records ------")
    invalidSchemaRecords.collect().foreach(println)

    println("\n---------------- valid Schema records ------")
    jsonRecordsWithProperSchema.collect().foreach(println)

  }

}
