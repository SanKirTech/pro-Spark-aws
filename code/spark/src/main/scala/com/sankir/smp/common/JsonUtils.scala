/*
 *
 *  * SanKir Technologies
 *  * (c) Copyright 2021.  All rights reserved.
 *  * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  *  without the prior written permission from Sankir Technologies.
 *  *
 *  * The course contents can be accessed by subscribing to pro-Spark course.
 *  *
 *  * Please visit www.sankir.com for details.
 *  *
 *
 */

package com.sankir.smp.common

import java.io.InputStream
import java.time.ZonedDateTime
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, LongType, StructType}

import scala.util.Try

/***
 * JsonNode objects for Int, String will be like IntNode, TextNode etc
 * You have to use asText() , asInt() to deserialize them to scala objects
 * so the process is
 * json -> JsonNode object --> scala object
 */
object JsonUtils {

  val MAPPER: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def jsonNodetoString(jsonNode: JsonNode): String =
    MAPPER.writeValueAsString(jsonNode)

  def jsonNodetoStringValue(value: Any): String =
    MAPPER.writeValueAsString(value)

  def jsonNodetoBytes(value: Any): Array[Byte] = MAPPER.writeValueAsBytes(value)

  def toJsonNode(bytes: Array[Byte]): JsonNode = MAPPER.readTree(bytes)

  def toJsonNode(is: InputStream): JsonNode = MAPPER.readTree(is)

  def toObjectNodeOptional(string: String): Option[ObjectNode] =
    toJsonNodeOptional(string).flatMap(
      node =>
        if (node.isObject) Option(node.asInstanceOf[ObjectNode])
        else Option.empty
    )

  def toJsonNodeOptional(string: String): Option[JsonNode] =
    Option(toJsonNode(string))

  def toJsonNode(jsonString: String): JsonNode = MAPPER.readTree(jsonString)

  def toCustomClass[T](string: String, objectType: Class[T]): T =
    MAPPER.readValue(string, objectType)

  def getObjectPropertyOptional(node: JsonNode,
                                property: String): Option[ObjectNode] = {
    val nodeProperty = node.get(property)
    if (nodeProperty != null && nodeProperty.isObject) {
      Option(nodeProperty.asInstanceOf[ObjectNode])
    } else Option.empty
  }

  def getArrayProperty(node: JsonNode, propertyName: String): ArrayNode =
    getArrayPropertyOptional(node, propertyName).getOrElse(
      throw requiredPropertyError(propertyName).apply
    )

  def getArrayPropertyOptional(node: JsonNode,
                               property: String): Option[ArrayNode] = {
    val nodeProperty = node.get(property)
    if (nodeProperty != null && nodeProperty.isArray) {
      Option(nodeProperty.asInstanceOf[ArrayNode])
    } else Option.empty
  }

  def getBooleanProperty(node: JsonNode, propertyName: String): Boolean =
    getBooleanPropertyOptional(node, propertyName).getOrElse(
      throw requiredPropertyError(propertyName).apply
    )

  def getBooleanPropertyOptional(node: JsonNode,
                                 property: String): Option[Boolean] = {
    val nodeProperty = node.get(property)
    if (nodeProperty != null && nodeProperty.isBoolean) {
      Option(nodeProperty.booleanValue())
    } else Option.empty
  }

  def getStringPropertyOptional(node: JsonNode,
                                property: String): Option[String] = {
    val nodeProperty = node.get(property)
    if (nodeProperty != null && nodeProperty.isTextual) {
      Option(nodeProperty.textValue())
    } else Option.empty
  }

  def getStringProperty(node: JsonNode, propertyName: String): String =
    getStringPropertyOptional(node, propertyName).getOrElse(
      throw requiredPropertyError(propertyName).apply
    )

  def getNotEmptyStringProperty(node: JsonNode, propertyName: String): String =
    getNotEmptyStringPropertyOptional(node, propertyName).getOrElse(
      throw requiredPropertyError(propertyName).apply
    )

  def requiredPropertyError(
                             propertyName: String
                           ): () => IllegalArgumentException =
    () => new IllegalArgumentException(s"$propertyName is required")

  def getNotEmptyStringPropertyOptional(node: JsonNode,
                                        propertyName: String): Option[String] =
    getStringPropertyOptional(node, propertyName).filter(_.trim.nonEmpty)

  def asStringPropertyOptional(node: JsonNode,
                               property: String): Option[String] = {
    val nodeProperty = node.get(property)
    if (nodeProperty != null) {
      Option(nodeProperty.asText())
    } else Option.empty
  }

  def asStringProperty(node: JsonNode, propertyName: String): String =
    asStringPropertyOptional(node, propertyName).getOrElse(
      throw requiredPropertyError(propertyName).apply
    )

  def getPropertyAsTextOptional(node: JsonNode,
                                property: String): Option[String] = {
    val nodeProperty = node.get(property)
    if (nodeProperty != null) {
      Option(nodeProperty.toString)
    } else Option.empty
  }

  def getPropertyAsText(node: JsonNode, propertyName: String): String =
    getPropertyAsTextOptional(node, propertyName).getOrElse(
      throw requiredPropertyError(propertyName).apply
    )

  def getTimestampPropertyOptional(node: JsonNode,
                                   property: String): Option[ZonedDateTime] =
    getStringPropertyOptional(node, property).map(ZonedDateTime.parse(_))

  def getTimestampProperty(node: JsonNode,
                           propertyName: String): ZonedDateTime =
    getTimestampPropertyOptional(node, propertyName).getOrElse(
      throw requiredPropertyError(propertyName).apply
    )

  def getLongProperty(node: JsonNode, propertyName: String): Long =
    getLongPropertyOptional(node, propertyName).getOrElse(
      throw requiredPropertyError(propertyName).apply
    )

  def getLongPropertyOptional(node: JsonNode,
                              property: String): Option[Long] = {
    val nodeProperty = node.get(property)
    if (nodeProperty != null && (nodeProperty.isIntegralNumber || nodeProperty.isFloatingPointNumber)) {
      Option(nodeProperty.longValue())
    } else Option.empty
  }

  def asLongPropertyOptional(node: JsonNode,
                             propertyName: String): Option[Long] =
    Options
      .or(
        getLongPropertyOptional(node, propertyName),
        getStringPropertyOptional(node, propertyName)
      )
      .flatMap(x => Try(x.toString.toLong).toOption)

  def emptyArray(): ArrayNode = MAPPER.createArrayNode()

  def emptyObject(): ObjectNode = MAPPER.createObjectNode()

  def convertToRow(jsonNode: JsonNode, schema: StructType) : Row = {
    Row.fromSeq(schema.fields.map( field => {
      val fieldValue = asStringProperty(jsonNode, field.name)
      field.dataType match {
        case LongType => fieldValue.toLong
        case DoubleType => fieldValue.toDouble
        case _ => fieldValue
      }
    }))
  }

}
