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

package com.sankir.smp.utils.encoders

import com.fasterxml.jackson.databind.JsonNode
import com.google.api.services.bigquery.model.TableRow
import com.sankir.smp.cloud.common.vos.ErrorTableRow
import com.sankir.smp.core.transformations.Insight.{RetailCase, kpiSchema}
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.util.Try

/***
 * Define all the implict parameters needed by Dataset
 */
object CustomEncoders {
  //implicit val stringEncoder: Encoder[String] = Encoders.STRING
  implicit val stringJsonNodeEncoder: Encoder[(String, JsonNode)] =
    Encoders.kryo[(String, JsonNode)]
  implicit val jsonNodeTupleEncoder: Encoder[(String, Try[JsonNode])] =
    Encoders.kryo[(String, Try[JsonNode])]
  implicit val tableRowEncoder: Encoder[TableRow] = Encoders.kryo[TableRow]
  //  implicit val retailEncoder = Encoders.product[RetailCase]
  //  implicit val kpiEncoder = Encoders.product[kpiSchema]
  implicit val jsonNodeEncoder: Encoder[JsonNode] = Encoders.kryo[JsonNode]
  implicit val errorTableRowEncoder: Encoder[ErrorTableRow] =
    Encoders.kryo[ErrorTableRow]

}
