package com.sankir.smp.cloud.aws

import com.sankir.smp.cloud.CloudConnector
import org.codehaus.jackson

object AWSConnector extends CloudConnector {
  override def saveToObjectStorage(path: String, data: String): Unit = ???

  override def readFromObjectStorage(path: String): String = ???

  override def saveToErrorTable(data: jackson.JsonNode): Unit = ???

  override def saveToErrorTable(datas: List[jackson.JsonNode]): Unit = ???

  override def saveToIngresTable(data: jackson.JsonNode): Unit = ???

  override def saveToIngressTable(datas: List[jackson.JsonNode]): Unit = ???
}
