package com.sankir.smp.cloud

import org.apache.spark.sql.{DataFrame, Dataset}
import org.codehaus.jackson.JsonNode

trait CloudConnector {

  def saveToObjectStorage(path: String, data: String)

  def readFromObjectStorage(path: String): String

  def saveToErrorTable(data: JsonNode)

  def saveToErrorTable(datas: List[JsonNode])

  def saveToErrorTable(df: DataFrame)

  def saveToIngressTable[T](ds: Dataset[T])

  def saveToIngressTable(datas: List[JsonNode])

  def saveToIngressTable(data: JsonNode)

  // def saveToIngressTable(df: DataFrame)

}
