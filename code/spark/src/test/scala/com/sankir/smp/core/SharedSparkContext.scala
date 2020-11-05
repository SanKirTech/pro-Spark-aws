package com.sankir.smp.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

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
