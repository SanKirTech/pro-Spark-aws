package com.sankir.smp.core

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.sankir.smp.cloud.aws.AWSConnector
import com.sankir.smp.cloud.common.CloudConnector
import com.sankir.smp.cloud.common.vos.CloudConfig
import com.sankir.smp.common.JsonUtils

/**
 * `CloudInitializer` will help us in creating the cloud connectors and cloud configs
 * <br> It will read the <b>/resources/application.yml</b> and will initialize the objects.
 *
 */
object CloudInitializer {
  var cloudConnector: CloudConnector = _

  var cloudConfig: CloudConfig = _

  def initializeCloud(): Unit = {
    val applicationYaml = getClass.getClassLoader.getResourceAsStream("application.yml")
    /**
     * ObjectMapper helps to serialize and deseriliaze Java Objects to JSON and vice versa
     */
    val MAPPER = new ObjectMapper(new YAMLFactory())
      .registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    /**
     * Here application.yml gets deseiralized to JsonNode java objects
     */
    val result: JsonNode = MAPPER.readTree(applicationYaml)
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
