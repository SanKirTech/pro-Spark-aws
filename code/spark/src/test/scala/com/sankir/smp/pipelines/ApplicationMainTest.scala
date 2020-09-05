package com.sankir.smp.pipelines

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

class ApplicationMainTest extends AnyFlatSpec  with SharedSparkContext {

  behavior of "ApplicationMain"

  it should "process normal file" in {
    val inputRdd = sparkSession.sparkContext.parallelize(Array("hello", "world"))
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
