package com.sankir.smp.cloud

import org.codehaus.jackson.JsonNode

trait CloudConnector {

  def saveToObjectStorage(path: String, data: String)

  def readFromObjectStorage(path: String): String

  def saveToErrorTable(data: JsonNode)

  def saveToErrorTable(datas: List[JsonNode])

  def saveToIngresTable(data: JsonNode)

  def saveToIngressTable(datas: List[JsonNode])

}
