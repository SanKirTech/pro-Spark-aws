package com.sankir.smp.pipelines

import com.fasterxml.jackson.databind.JsonNode
import com.google.api.services.bigquery.model.TableRow
import com.sankir.smp.utils.CmdLineOptions
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.Try

class ApplicationMainTest extends AnyFlatSpec  with SharedSparkContext {

  behavior of "ApplicationMain"

  it should "able to process invalid jsons" in {
    val CONFIG = CmdLineOptions(
      projectId = "sankir-1705",
      inputLocation = "F:\\extra-work\\lockdown_usecases\\SparkUsecase\\code\\spark\\input.json",
      //      schemaLocation = "F:\\extra-work\\lockdown_usecases\\SparkUsecase\\infrastructure\\terraforms\\project\\json-schema\\t_transaction.json"
      schemaLocation = "./t_transaction.json"
    )
    val inputRdd = sparkSession.sparkContext.parallelize(Array("hello", "world"))
    implicit val jsonNodeEncoder = Encoders.kryo[(String, Try[JsonNode])]
    implicit val tableRowEncoder = Encoders.kryo[TableRow]
    val wordCount = inputRdd.map((_,1)).reduceByKey( _ + _)
    assert(wordCount.count() == 2)
  }

}

trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>
  @transient private var _spark: SparkSession = _

  def sparkSession: SparkSession = _spark

  var conf = new SparkConf(false)

  override protected def beforeAll(): Unit = {
    _spark = SparkSession.builder().appName("Test").master("local[2]").getOrCreate()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    _spark.stop()
    _spark = null
    super.afterAll()
  }
}
