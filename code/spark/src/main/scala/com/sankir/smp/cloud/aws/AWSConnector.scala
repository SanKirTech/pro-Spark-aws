package com.sankir.smp.cloud.aws

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.cloud.common.CloudConnector
import com.sankir.smp.cloud.common.vos.{CloudConfig, ErrorTableRow}
import com.sankir.smp.common.JsonUtils._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

/**
 * AWSConnecetor is the implementation of CloudConnector trait with AWS specific APIs.
 * @param cloudConfig
 * @param configuration
 */

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

  override def readFromObjectStorage(path: String): String = {
    s3io.getData(path)
  }

  override def saveError(ds: Dataset[ErrorTableRow]): Unit = {
    val error = asStringProperty(persistentStorageConfig, "error")
    if (isPersistentTypeObject) {
      ds.toDF.write.format(CSV).mode(SaveMode.Append).save(error)
    }
  }

  override def saveIngress[T](ds: Dataset[T]): Unit = {
    val ingress = asStringProperty(persistentStorageConfig, "ingress")
    if (isPersistentTypeObject) {
      ds.toDF.write.format(CSV).mode(SaveMode.Overwrite).save(ingress)
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