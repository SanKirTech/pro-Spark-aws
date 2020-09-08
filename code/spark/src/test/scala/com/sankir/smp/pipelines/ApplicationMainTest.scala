package com.sankir.smp.pipelines

import com.sankir.smp.app.JsonUtils
import com.sankir.smp.pipelines.validators.Validator.{jsonSchemaValidator, jsonStringValidator}
import com.sankir.smp.utils.Resources.{readAsString, readAsStringIterator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, Suite}

class ApplicationMainTest extends AnyFlatSpec with SharedSparkContext {

  behavior of "Application"

  it should "should convert invalid jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(readAsStringIterator("pipelines/invalid_json_data.txt").toSeq)
    val jsonValidatedRecords = jsonStringValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isFailure).count() == 4)
  }

  it should "should convert valid jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(readAsStringIterator("pipelines/valid_json_data.txt").toSeq)
    val jsonValidatedRecords = jsonStringValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isSuccess).count() == 3)
  }

  it should "should convert invalid schema jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(readAsStringIterator("pipelines/schema_json_data.txt").toSeq)
    val schema = readAsString("pipelines/schema.json")
    val jsonValidatedRecords = sdfData.map(rec => (rec, JsonUtils.deserialize(rec)))
    val schemaValidatedRecords = jsonSchemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isFailure).count() == 1)
  }

  it should "should convert valid schema jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(readAsStringIterator("pipelines/schema_json_data.txt").toSeq)
    val schema = readAsString("pipelines/schema.json")
    val jsonValidatedRecords = sdfData.map(rec => (rec, JsonUtils.deserialize(rec)))
    val schemaValidatedRecords = jsonSchemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isSuccess).count() == 2)
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
