package com.sankir.smp.vo

case class BigTableErrorRows(
                    timestamp: String,
                    payload:String,
                    jobName: String,
                    errorMessage: String,
                    stackTrace: String
                    )
