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

package com.sankir.smp.connectors

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{BlobId, StorageException, StorageOptions}

case class GcsIO(var googleCredentials: ServiceAccountCredentials = null,
                 projectId: String) {

  val storageClient =
    if (googleCredentials ne null) {
      StorageOptions
        .newBuilder()
        .setCredentials(googleCredentials)
        .setProjectId(projectId)
        .build()
        .getService
    } else {
      StorageOptions.getDefaultInstance.getService
    }

  /***
    *
    * @param path takes path of the file in GCS determines the storage bucket from it
    *             gets the blobid gets the content
    * @throws exeption if not able to find the object
    * @return - returns the content of the file
    */
  @throws[StorageException]
  def getData(path: String): String = {
    val gcsObject =
      getBucketAndPath(path).getOrElse(GCSObject(bucketName = "", path = ""))
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

  /***
    *
    * @param path path of the file whose content is required to be read
    * @return  returns a GCSObject with bucket and path of the object
    */
  def getBucketAndPath(path: String): Option[GCSObject] = {
    val gcsBucketRegex = "gs://(.+?)/(.+)".r
    path match {
      case gcsBucketRegex(bucket, path) => Some(GCSObject(bucket, path))
      case _                            => None
    }
  }

}

case class GCSObject(bucketName: String, path: String)

case class ObjectNotFoundException(message: String) extends Exception
