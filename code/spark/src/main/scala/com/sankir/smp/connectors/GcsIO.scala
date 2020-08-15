package com.sankir.smp.connectors

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{BlobId, StorageOptions}

case class GcsIO(var googleCredentials: ServiceAccountCredentials, projectId: String) {
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

//  def getData(path: String): String = {
//    val blobId = Blo
//    storageClient.get()
//  }
}
