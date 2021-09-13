package com.sankir.smp.cloud.common

import com.sankir.smp.cloud.common.vos.ErrorTableRow
import com.sankir.smp.utils.enums.ErrorEnums.ErrorEnums

import java.time.Instant
import scala.util.Try

object CloudConverter {
  def convertToErrorTableRow[A](errorRecord: (String, Try[A]),
                                errorType: ErrorEnums,
                                appName: String): ErrorTableRow = {
    val errorTableRow = ErrorTableRow(
      timestamp = Instant.now().toString,
      errorType = errorType.toString,
      payload = errorRecord._1,
      stackTrace = errorRecord._2.failed.get.getStackTrace.mkString,
      jobName = appName,
      errorMessage = errorRecord._2.failed.get.getMessage
    )
    errorTableRow
  }
}
