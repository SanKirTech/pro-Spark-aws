package com.sankir.smp.core

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sankir.smp.cloud.aws.AWSConnector
import com.sankir.smp.cloud.common.CloudConnector
import com.sankir.smp.cloud.common.vos.CloudConfig

/**
 * `CloudInitializer` will help us in creating the cloud connectors and cloud configs
 * <br> It will read the <b>/resources/application.yaml</b> and will initialize the objects.
 *
 */
object CloudInitializer {
  var cloudConnector: CloudConnector = _

  var cloudConfig: CloudConfig = _

  def initializeCloud(): Unit = {
    val data = getClass.getClassLoader.getResourceAsStream("application.yaml")
    val MAPPER = new ObjectMapper(new YAMLFactory())
      .registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val result = MAPPER.readTree(data)
    cloudConfig = MAPPER.treeToValue(result, classOf[CloudConfig])

    result.get("cloudType").asText() match {
      case "aws" =>
        cloudConnector = AWSConnector(
          cloudConfig,
          result.get("aws")
          )
    }
  }
}
