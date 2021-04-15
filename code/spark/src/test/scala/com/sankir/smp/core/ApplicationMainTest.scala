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

package com.sankir.smp.core

import com.sankir.smp.common.JsonUtils
import com.sankir.smp.core.validators.DataValidator.{
  jsonValidator,
  schemaValidator
}
import com.sankir.smp.utils.FileSource.{readAsString, readAsStringIterator}
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder

class ApplicationMainTest extends AnyFlatSpec with SharedSparkContext {

  behavior of "Application"
  implicit val stringEncoder: Encoder[String] = Encoders.STRING
  it should "should convert invalid jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/invalid_data_main.json").toSeq
    )
    val jsonValidatedRecords = jsonValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isFailure).count() == 4)
  }

  it should "should convert valid jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/valid_data_main.json").toSeq
    )
    val jsonValidatedRecords = jsonValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isSuccess).count() == 5)
  }

  it should "should convert invalid schema jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/data_valid_schema_main.json").toSeq
    )
    val schema = readAsString("core/valid_schema_main.json")
    val jsonValidatedRecords =
      sdfData.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val schemaValidatedRecords = schemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isFailure).count() == 2)
  }

  it should "should convert valid schema jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(
      readAsStringIterator("core/data_valid_schema_main.json").toSeq
    )
    val schema = readAsString("core/valid_schema_main.json")
    val jsonValidatedRecords =
      sdfData.map(rec => (rec, JsonUtils.toJsonNode(rec)))
    val schemaValidatedRecords = schemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isSuccess).count() == 1)
  }

}
