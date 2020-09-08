package com.sankir.smp.connectors

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{BlobId, StorageException, StorageOptions}

case class GcsIO(var googleCredentials: ServiceAccountCredentials = null, projectId: String) {
  val storageClient =
    if (googleCredentials ne null) {
      StorageOptions.newBuilder()
        .setCredentials(googleCredentials)
        .setProjectId(projectId)
        .build()
        .getService
    } else {
      StorageOptions.getDefaultInstance.getService
    }

  @throws[StorageException]
  def getData(path: String): String = {
    val gcsObject = getBucketAndPath(path).getOrElse(GCSObject(bucketName = "", path = ""))
    val blobId = BlobId.of(gcsObject.bucketName, gcsObject.path)
    try {
      if (storageClient.get(blobId) ne null)
        new String(storageClient.get(blobId).getContent())
      else
        throw ObjectNotFoundException(s"Not able to find object in $path")
    } catch {
      case storageException: StorageException =>
        throw storageException
    }
  }

  def getBucketAndPath(path: String): Option[GCSObject] = {
    val gcsBucketRegex = "gs://(.+?)/(.+)".r
    path match {
      case gcsBucketRegex(bucket, path) => Some(GCSObject(bucket, path))
      case _ => None
    }
  }

}

case class GCSObject(bucketName: String, path: String)

case class ObjectNotFoundException(message: String) extends Exception
