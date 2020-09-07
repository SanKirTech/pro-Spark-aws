package com.sankir.smp.common.converters

import java.time.Instant

import com.fasterxml.jackson.databind.JsonNode
import com.google.api.services.bigquery.model.TableRow
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.utils.enums.ErrorEnums.ErrorEnums
import com.sankir.smp.vo.BigTableErrorRows

import scala.util.Try

object Converter {

  def convertAToTryB[A,B](a: A, fun: (A) => B) : Try[B] =
    Try(fun(a))

  def convertABToTryB[A, B](a: A, b: B, fun: (A,B)=> B) : Try[B] =
    Try(fun(a,b))

//  def convertToValidatedJsonNodeTuple()

  def convertToErrorTableRows[A](errorRecord: (String, Try[A]), errorType: ErrorEnums, appName: String): TableRow = {
    val bigTableErrorRow = BigTableErrorRows(
      timestamp = Instant.now().toString,
      errorType = errorType.toString,
      payload = errorRecord._1,
      stackTrace = errorRecord._2.failed.get.getStackTrace.mkString,
      jobName = appName,
      errorMessage = errorRecord._2.failed.get.getMessage
    )
    JsonUtils.MAPPER.convertValue(bigTableErrorRow, classOf[TableRow])
  }

}