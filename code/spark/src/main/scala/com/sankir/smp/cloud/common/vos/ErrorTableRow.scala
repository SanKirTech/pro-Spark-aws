package com.sankir.smp.cloud.common.vos

case class ErrorTableRow(errorReplay: String = "",
                         timestamp: String,
                         errorType: String,
                         payload: String,
                         jobName: String,
                         errorMessage: String,
                         stackTrace: String)
