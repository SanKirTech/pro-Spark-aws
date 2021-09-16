package com.sankir.smp.cloud.common

case class CloudConfig(inputLocation: String,
                       schemaLocation: String,
                       ingressTable: String,
                       errorTable: String,
                       objectStorage: String,
                       kpiLocation: String)
