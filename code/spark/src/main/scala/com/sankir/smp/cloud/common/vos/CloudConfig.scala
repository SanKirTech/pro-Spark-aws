package com.sankir.smp.cloud.common.vos

/**
  * CloudConfig is a POJO closs that reads the data from application.yaml<br>
  * It is created from {@link com.sankir.smp.core.CloudInitializer CloudInitializer}
  */
case class CloudConfig(inputLocation: String,
                       schemaLocation: String,
                       ingressTable: String,
                       errorTable: String,
                       objectStorage: String,
                       kpiLocation: String,
                       tempKPIViewName: String)
