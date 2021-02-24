/*
 * Comment for pipeline module
 * Scala objects
 * Spark sql table
 * KPI tables
 *
 */

package com.sankir.smp.common

import java.time.Instant

import com.google.api.services.bigquery.model.TableRow
import com.sankir.smp.utils.enums.ErrorEnums.ErrorEnums
import com.sankir.smp.core.transformations.ErrorTransformations._

import scala.util.Try

object Converter {

  /***
    *
    * @param a    is of Data Type A
    * @param fun  pass function fun as argument which takes 2 parameters
    *            of types A and B
    * @tparam A type parameter A
    * @tparam B type parameter B
    * @return
    */
  def convertAToTryB[A, B](a: A, fun: A => B): Try[B] =
    Try(fun(a))

  /***
    *
    * @param a - a is of Data Type A
    * @param b - b is of Data Type B
    * @param fun - pass function fun as argument which takes 2 parameters
    *            of types A and B
    * @tparam A - type parameter A
    * @tparam B - type parameter B
    * @return returns Try[B]
    */
  def convertABToTryB[A, B](a: A, b: B, fun: (A, B) => B): Try[B] =
    Try(fun(a, b))

//  def convertToValidatedJsonNodeTuple()

  def convertToErrorTableRows[A](errorRecord: (String, Try[A]),
                                 errorType: ErrorEnums,
                                 appName: String): TableRow = {
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
