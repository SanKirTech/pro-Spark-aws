package com.sankir.smp.pipelines

import java.io.IOException
import java.util.concurrent.{ExecutionException, TimeUnit}

import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{PubsubMessage, TopicName}
import com.sankir.smp.app.Node
import com.sankir.smp.connectors.PubSubIO
import org.apache.spark.sql.SparkSession


object ApplicationMain {

  def main(args: Array[String]): Unit = {
//    val sparkSession = SparkSession.builder().appName("ProSparkBatch").master("local[*]").getOrCreate();

//        sparkSession.read.textFile("").write.;

    //TODO: Read data from GCS Location into a Dataset
    //TODO: Convert json String to SDF object [ Data Validation]
    //TODO: Rule Validation
    //TODO: Store correct data in Big Query
    //TODO: Store error data in Big Query
    //TODO: Send data to pubSub


    //    val inputPath = args(0)
    //    val outputPath = args(1)
    //
    //    val sc = new SparkContext(new SparkConf().setAppName("Word Count"))
    //    val lines = sc.textFile(inputPath)
    //    val words = lines.flatMap(line => line.split(" "))
    //    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    //    wordCounts.saveAsTextFile(outputPath)

//    publisherExample("sankir-1705", "projects/sankir-1705/topics/sample")
    val pubSubIO = PubSubIO("sankir-1705","sample")
//    pubSubIO.publishMessage("hello world")

    val jsonString = "{\"error\": \"schema-validation-error\", \"message\": \"not a valid string\"}"

    val errorMessageInJson = Node.deserialize(jsonString)

    pubSubIO.publishMessage(errorMessageInJson.asInstanceOf[ObjectNode])
    pubSubIO.close()


  }




}
