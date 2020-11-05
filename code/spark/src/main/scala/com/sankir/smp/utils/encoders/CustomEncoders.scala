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

package com.sankir.smp.utils.encoders

import com.fasterxml.jackson.databind.JsonNode
import com.google.api.services.bigquery.model.TableRow
import org.apache.spark.sql.{Encoder, Encoders}

import scala.util.Try

object CustomEncoders {
  implicit val stringEncoder: Encoder[String] = Encoders.STRING
  implicit val stringJsonNodeEncoder: Encoder[(String,JsonNode)] = Encoders.kryo[(String,JsonNode)]
  implicit val jsonNodeTupleEncoder: Encoder[(String, Try[JsonNode])] = Encoders.kryo[(String, Try[JsonNode])]
  implicit val tableRowEncoder: Encoder[TableRow] = Encoders.kryo[TableRow]
}
