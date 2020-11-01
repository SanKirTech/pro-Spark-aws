package com.sankir.smp.pipelines

import com.jayway.jsonpath.JsonPath
import com.sankir.smp.app.JsonUtils
import com.sankir.smp.pipelines.transformations.Insight
import com.sankir.smp.pipelines.validators.Validator.{businessRuleValidator, jsonValidator}
import com.sankir.smp.utils.Resources.{readAsString, readAsStringIterator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json.JSONArray
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.scalatest.flatspec.AnyFlatSpec

class ValidatorTest extends AnyFlatSpec with SharedSparkContext {
  behavior of "BusinessRuleValidator"

  it should "return Sucess for valid json" in {
    import com.sankir.smp.utils.encoders.CustomEncoders._
//    val sdfData = sparkSession.createDataset(readAsStringIterator("pipelines/valid_json_data.txt").toSeq)
    println(readAsString("validators/data.json"))
    val sdfRdd = sparkSession.createDataset(readAsStringIterator("validators/data.json").toSeq)
    sdfRdd.show()

    val sdfDF = sparkSession.read.json(sdfRdd)

    sdfDF.show()


    businessRuleValidator(sdfDF, List("Quantity > 0", "Quantity == 5 and UnitPrice > 2")).show()

  }

}


