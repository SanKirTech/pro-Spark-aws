package com.sankir.smp.connectors


import java.io.IOException
import java.util.concurrent.{ExecutionException, TimeUnit}

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.cloud.pubsub.v1.Publisher
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import org.slf4j.LoggerFactory

case class PubSubIO(projectId: String, topicId: String) {

  val LOG = LoggerFactory.getLogger(PubSubIO.getClass)

  val publisher = Publisher.newBuilder(TopicName.of(projectId,topicId)).build()

  @throws[IOException]
  @throws[ExecutionException]
  @throws[InterruptedException]
  def publishMessage(message: String): Unit = {
    val data = ByteString.copyFromUtf8(message)
    val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
    val future=  publisher.publish(pubsubMessage)

    ApiFutures.addCallback(
      future,
      new ApiFutureCallback[String] {
        override def onFailure(throwable: Throwable): Unit = {
          LOG.error(s"$throwable")
        }
        override def onSuccess(messageId: String): Unit = {
          LOG.info(s"Messge published: $messageId")
        }
      },
      MoreExecutors.directExecutor()
    )
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
