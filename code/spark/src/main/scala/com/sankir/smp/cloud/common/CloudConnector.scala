package com.sankir.smp.cloud.common

import com.sankir.smp.cloud.common.vos.ErrorTableRow
import org.apache.spark.sql.{DataFrame, Dataset}

trait CloudConnector {

  def readFromObjectStorage(path: String): String

  def saveError(ds: Dataset[ErrorTableRow])

  def saveIngress[T](ds: Dataset[T])

  def saveKPI(df: DataFrame, kpiResultLocation: String)

}
