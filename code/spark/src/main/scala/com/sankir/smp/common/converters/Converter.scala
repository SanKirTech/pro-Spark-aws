package com.sankir.smp.common.converters

import java.time.Instant

import com.fasterxml.jackson.databind.JsonNode
import com.google.api.services.bigquery.model.TableRow
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.vo.BigTableErrorRows

import scala.util.{Failure, Try}

object Converter {

  def convertToJsonNodeTuple(jsonString: String): (String, Try[JsonNode]) =
    (jsonString, Try(JsonUtils.deserialize(jsonString)))

  def  convertToErrorTableRows[A](errorRecord: (String, Try[A]), appName:String) : TableRow = {
    BigTableErrorRows(
      timestamp = Instant.now().toString,
      payload = errorRecord._1,
      stackTrace = errorRecord._2.failed.get.getStackTrace.mkString,
      jobName = appName,
      errorMessage= errorRecord._2.failed.get.getMessage
    )
    JsonUtils.MAPPER.convertValue(BigTableErrorRows,classOf[TableRow])
  }

}