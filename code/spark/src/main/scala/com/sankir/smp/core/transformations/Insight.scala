/*
 * SanKir Technologies
 * (c) Copyright 2020.  All rights reserved.
 * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  without the prior written permission from Sankir Technologies.
 *
 * The course contents can be accessed by subscribing to pro-Spark course.
 *
 * Please visit www.sankir.com for details.
 *
 */

package com.sankir.smp.core.transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  DateType,
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType
}

object Insight {

//  val RetailCase = StructType(
//    Array(
//      StructField("InvoiceNo", StringType, nullable = false),
//      StructField("StockCode", StringType, nullable = false),
//      StructField("Description", StringType, nullable = false),
//      StructField("Quantity", IntegerType, nullable = false),
//      StructField("InvoiceDate", DateType, nullable = false),
//      StructField("UnitPrice", DoubleType, nullable = false),
//      StructField("CustomerID", DoubleType, nullable = false),
//      StructField("Country", StringType, nullable = false)
//    )
//  )

  /***
    *
    * @param InvoiceNo
    * @param StockCode
    * @param Description
    * @param Quantity
    * @param InvoiceDate
    * @param UnitPrice
    * @param CustomerID
    * @param Country
    */
  case class RetailCase(InvoiceNo: String,
                        StockCode: String,
                        Description: String,
                        Quantity: BigInt,
                        InvoiceDate: String,
                        UnitPrice: Double,
                        CustomerID: Double,
                        Country: String)
  /*
{"kpiindex":"k1",
"kpitable":"retail_kpi.t_sku_dow_daily1",
"kpiquery":"select distinct stockcode,
sum(round(quantity * unitprice)) ov0er w as revenue,dayofweek(InvoiceDate)
as Day_Of_Week, country from %s window w as (partition by stockcode,
country order by dayofweek(InvoiceDate),country) order by revenue desc, Day_Of_Week,country" }
   */
  case class kpiSchema(kpiindex: String, kpitable: String, kpiquery: String)

  /***
    * writekpiToBigQ
    * @param sparkSession
    * @param kq
    * @param bqtbl
    * @return
    */
  def writekpiToBigQ(sparkSession: SparkSession,
                     kq: String,
                     bqtbl: String): Option[String] = {
    val bucket = "sankir-storage-prospark"
    sparkSession.conf.set("temporaryGcsBucket", bucket)
    val kpiDF = sparkSession.sql(kq)
    kpiDF.printSchema()

    kpiDF.show(100, false)
    kpiDF.write.format("bigquery")
    kpiDF
      .coalesce(5)
      .write
      .format("bigquery")
      .mode("overwrite")
      .save(bqtbl)

    Some("Success")
  }

  /***
    * method name: runKPIQuery
    * @param sparkSession - instance of the Spark Session
    * @param sparkTable - spark table in global_temp schema
    * @param kpiLocation - Location of the kpi json file
    * @return unit - does not return anything
    */
  def runKPIQuery(sparkSession: SparkSession,
                  sparkTable: String,
                  kpiLocation: String): Unit = {

    println("KPI Location in GCP : " + kpiLocation)
    var qString: String = "" //Query String to be populated
    var qTable: String = "" //BigQUery table to be used for storing the results
    var qDisc: String = "" //KPI Description

    //val queryDS = sparkSession.read.json(kpiLocation)
    import sparkSession.implicits._
    val queryDS = sparkSession.read.json(kpiLocation).as[kpiSchema]

    val kpiIndices = List("k1a", "k1b", "k2", "k3", "k4", "k5", "k6", "k7")

    def kpiPrint(kpiIndex: String): Unit = {

      val filterCondition = "kpiindex = '%s'".format(kpiIndex)

      qString = queryDS
        .where(filterCondition)
        .select("kpiquery")
        .first()
        .get(0)
        .toString

      qTable = queryDS
        .where(filterCondition)
        .select("kpitable")
        .first()
        .get(0)
        .toString
      qDisc = queryDS
        .where(filterCondition)
        .select("kpidiscription")
        .first()
        .get(0)
        .toString

      println("----------------------------------------------------")
      println(kpiIndex + ":" + qTable + ":" + qDisc)
      println("----------------------------------------------------")
      println("Query String")
      println(qString)
      println(
        Insight
          .writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable)
          .getOrElse("Failed in %s".format(kpiIndex))
      )
    }

    kpiIndices.foreach(kpiPrint)
  }
}
