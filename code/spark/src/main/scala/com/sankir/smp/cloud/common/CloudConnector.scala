package com.sankir.smp.cloud.common

import com.sankir.smp.cloud.common.vos.ErrorTableRow
import org.apache.spark.sql.{DataFrame, Dataset}

trait CloudConnector {

  def readFromObjectStorage(path: String): String

  def saveToErrorTable(ds: Dataset[ErrorTableRow])

  def saveToIngressTable[T](ds: Dataset[T])

  def saveToKPITable(df: DataFrame, kpiTableName: String)

  def saveToKPILocation(df: DataFrame, kpiLocation: String)

}
