package com.sankir.smp.vo


case class BigTableErrorRows(
                    errorReplay: String = "",
                    timestamp: String,
                    errorType: String,
                    payload:String,
                    jobName: String,
                    errorMessage: String,
                    stackTrace: String
                    )
