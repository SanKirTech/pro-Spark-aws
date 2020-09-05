package com.sankir.smp.connectors


import java.io.IOException
import java.util.concurrent.{ExecutionException, TimeUnit}

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.api.gax.core.{CredentialsProvider, FixedCredentialsProvider}
import com.google.auth.Credentials
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.pubsub.v1.Publisher
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import org.slf4j.LoggerFactory

case class PubSubIO(projectId: String, topicId: String, var googleCredentials: ServiceAccountCredentials = null) {

  val LOG = LoggerFactory.getLogger(PubSubIO.getClass)

  val publisher =
    Publisher
      .newBuilder(TopicName.of(projectId, topicId))
      .setCredentialsProvider(
        if (googleCredentials ne null) FixedCredentialsProvider.create(googleCredentials)
        else FixedCredentialsProvider.create(GoogleCredentials.getApplicationDefault())
      )
      .build()

  @throws[IOException]
  @throws[ExecutionException]
  @throws[InterruptedException]
  def publishMessage(message: String): String = {
    val data = ByteString.copyFromUtf8(message)
    val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
    val future = publisher.publish(pubsubMessage)
    try {
      future.get()
    } catch {
      case exception: Exception => {
        LOG.error(s"Unable to send the data to pub sub ${exception.getMessage}")
        throw exception
      }
    }

  }

  def publishMessage(message: ObjectNode): Unit = {
    publishMessage(message.toString)
  }

  def close(): Unit =
    if (publisher ne null) {
      publisher.shutdown()
      publisher.awaitTermination(1, TimeUnit.MINUTES)
    }
}
