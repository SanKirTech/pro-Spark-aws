package com.sankir.smp.core

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sankir.smp.cloud.CloudConfig
import com.sankir.smp.cloud.gcp.GcpConnector

object TestMain {
  def main(args: Array[String]): Unit = {
    val data = getClass.getClassLoader.getResourceAsStream("application.yaml")
    val MAPPER = new ObjectMapper(new YAMLFactory())
      .registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val result = MAPPER.readTree(data)
    println(result)

    val storageConfig = MAPPER.treeToValue(result, classOf[CloudConfig])

    result.get("cloud_type").asText() match {
      case "gcp" =>
        GcpConnector(MAPPER.convertValue(result.get("gcp"), new TypeReference[Map[String, Any]] {}))
    }
    print(storageConfig)
  }
}
