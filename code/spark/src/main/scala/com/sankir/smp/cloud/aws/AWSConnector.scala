package com.sankir.smp.cloud.aws

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.cloud.common.CloudConnector
import com.sankir.smp.cloud.common.vos.{CloudConfig, ErrorTableRow}
import com.sankir.smp.common.JsonUtils._
import com.sankir.smp.common.{JsonUtils, Matcher}
import com.sankir.smp.common.Matchers.and
import com.sankir.smp.core.ProSparkApp.logInfo
import com.sankir.smp.utils.encoders.CustomEncoders.errorTableRowEncoder
import org.apache.commons.lang3.StringUtils.isNotEmpty
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import java.nio.file.{Files, Paths}

final case class AWSConnector(cloudConfig: CloudConfig,
                              configuration: JsonNode)
  extends CloudConnector
    with Logging
    with Serializable {

  private val CSV = "csv"

  private def createS3Client(): AmazonS3 = {
    AmazonS3ClientBuilder
      .standard()
      .withRegion(Regions.US_EAST_2)
      .build()
  }

  private val s3io = S3IO(createS3Client())
  override def saveToObjectStorage(path: String, data: String): Unit = {
    s3io.writeToS3(path, data)
  }

  override def readFromObjectStorage(path: String): String = {
    s3io.getData(path)
  }

  override def saveError(ds: Dataset[ErrorTableRow]): Unit = {
    val error = asStringProperty(persistentStorageConfig, "error")
    if (isPersistentTypeObject) {
      //ds.toDF.write.format(CSV).mode(SaveMode.Append).save(error)
      s3io.writeToS3("s3a://retail-sankir/error", ds.as[ErrorTableRow].toString())
    }
  }

  override def saveIngress[T](ds: Dataset[T]): Unit = {
    val ingress = asStringProperty(persistentStorageConfig, "ingress")
    if (isPersistentTypeObject) {
      ds.write.format(CSV).mode(SaveMode.Overwrite).save(ingress)
    }
  }

  override def saveKPI(df: DataFrame, kpiTable: String): Unit = {
    val kpi = asStringProperty(persistentStorageConfig, "kpi")
    if (isPersistentTypeObject) {
      df.write.format(CSV).mode(SaveMode.Overwrite).save(s"$kpi/$kpiTable")
    }
  }

  private val persistentStorageConfig = configuration.get("persistentStorage")
  private val isPersistentTypeObject: Boolean =
    asStringProperty(persistentStorageConfig, "type") == "object"

}