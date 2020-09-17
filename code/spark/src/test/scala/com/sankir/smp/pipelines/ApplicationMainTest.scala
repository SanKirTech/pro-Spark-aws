package com.sankir.smp.pipelines

import com.sankir.smp.app.JsonUtils
import com.sankir.smp.pipelines.validators.Validator.{schemaValidator, jsonValidator}
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
    val jsonValidatedRecords = jsonValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isFailure).count() == 4)
  }

  it should "should convert valid jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(readAsStringIterator("pipelines/valid_json_data.txt").toSeq)
    val jsonValidatedRecords = jsonValidator(sdfData)
    assert(jsonValidatedRecords.filter(_._2.isSuccess).count() == 3)
  }

  it should "should convert invalid schema jsons to Failure objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(readAsStringIterator("pipelines/schema_json_data.txt").toSeq)
    val schema = readAsString("pipelines/schema.json")
    val jsonValidatedRecords = sdfData.map(rec => (rec, JsonUtils.deserialize(rec)))
    val schemaValidatedRecords = schemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isFailure).count() == 1)
  }

  it should "should convert valid schema jsons to Success objects" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(readAsStringIterator("pipelines/schema_json_data.txt").toSeq)
    val schema = readAsString("pipelines/schema.json")
    val jsonValidatedRecords = sdfData.map(rec => (rec, JsonUtils.deserialize(rec)))
    val schemaValidatedRecords = schemaValidator(jsonValidatedRecords, schema)
    assert(schemaValidatedRecords.filter(_._2.isSuccess).count() == 2)
  }

  it should "Actual Data" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
    val sdfData = sparkSession.createDataset(readAsStringIterator("pipelines/SixRecs.json").toSeq)

    println("\n--------  sdfData ------------")
    sdfData.show(20,false)

    val schema = readAsString("pipelines/t_transaction_trimmed.json")

    val jsonValidatedRecords = jsonValidator(sdfData)
    val jsonRecords = jsonValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    val inValidJsonRecords = jsonValidatedRecords.filter(_._2.isFailure)
   // writeToBigQuery(inValidJsonRecords, CMDLINEOPTIONS, JOBNAME, INVALID_JSON_ERROR)
    println("\n--------------- invalid JSON records -------------")
    inValidJsonRecords.collect().foreach(println)

    println("\n--------------- valid JSON records ---------------")
    jsonRecords.collect().foreach(println)

    val schemaValidatedRecords = schemaValidator(jsonRecords, schema)
    val jsonRecordsWithProperSchema = schemaValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2))
    val invalidSchemaRecords = schemaValidatedRecords.filter(_._2.isFailure)
    //writeToBigQuery(invalidSchemaRecords, CMDLINEOPTIONS, JOBNAME, INVALID_SCHEMA_ERROR)

    println("\n---------------- invalid Schema records ------")
    invalidSchemaRecords.collect().foreach(println)

    println("\n---------------- valid Schema records ------")
    jsonRecordsWithProperSchema.collect().foreach(println)




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
