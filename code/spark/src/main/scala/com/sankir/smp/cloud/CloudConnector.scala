package com.sankir.smp.cloud

import com.sankir.smp.cloud.common.vos.ErrorTableRow
import com.sankir.smp.utils.enums.ErrorEnums.ErrorEnums
import org.apache.spark.sql.{DataFrame, Dataset}
import org.codehaus.jackson.JsonNode

trait CloudConnector {

  def readFromObjectStorage(path: String): String

  def saveToErrorTable(ds: Dataset[ErrorTableRow])

  def saveToIngressTable[T](ds: Dataset[T])

}
