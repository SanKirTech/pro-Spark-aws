package com.sankir.smp.cloud.aws

import com.amazonaws.services.s3.AmazonS3

/**
 *
 * @param storageClient takes in a storageClient
 */
final case class S3IO(storageClient: AmazonS3) {

  /**
   *
   * @param path
   * @param data
   * Takes in path and data to be written and writes it to a file in S3
   */
  def writeToS3(path: String, data: String): Unit = {
    val bucketObject =
      getBucketAndPath(path)
        .getOrElse(throw new RuntimeException("Path is not valid :" + path))
    import java.nio.file.Paths
    val key_name = Paths.get(bucketObject.path).getFileName.toString
    storageClient.putObject(bucketObject.bucketName,key_name, bucketObject.path)
  }

  /**
   *
   * @param path
   * @return contents of the file
   */
  def getData(path: String): String = {
    val bucketObject =
      getBucketAndPath(path)
        .getOrElse(throw new RuntimeException("Path is not valid" + path))
    storageClient.getObjectAsString(bucketObject.bucketName, bucketObject.path)
  }

  private def getBucketAndPath(path: String): Option[BucketObject] = {
    val awsBucketRegex = "s3a://(.+?)/(.+)".r
    path match {
      case awsBucketRegex(bucket, path) => Some(BucketObject(bucket, path))
      case _                            => None
    }
  }

}
final case class BucketObject(bucketName: String, path: String)
