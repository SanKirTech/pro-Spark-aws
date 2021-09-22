package com.sankir.smp.cloud.common.vos

/**
  * CloudConfig is a POJO closs that reads the data from application.yaml<br>
  * It is created from {@link com.sankir.smp.core.CloudInitializer CloudInitializer}
  */
case class CloudConfig(runLocal: Boolean,
                       inputLocation: String,
                       schemaLocation: String,
                       kpiLocation: String,
                       tempKPIViewName: String,
                       sparkConfig: Map[String, String])
