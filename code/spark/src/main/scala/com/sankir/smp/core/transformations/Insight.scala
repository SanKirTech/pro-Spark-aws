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

//  val retailSchema = StructType(
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
  case class retailSchema(InvoiceNo: String,
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
    // kpiDF.write.format("bigquery")
//    kpiDF.coalesce(5).write.format("bigquery")
//      .mode("overwrite")
//      .save(bqtbl)
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
    //val queryDS = sparkSession.read.json(kpiLocation)
    import sparkSession.implicits._
    val queryDS = sparkSession.read.json(kpiLocation).as[kpiSchema]

    println(
      "KPI1: Highest selling SKUs on a daily basis (M,T,W,Th,F,S,Su) per country"
    )
    println("----------------------------------------------------")
    qString = queryDS
      .where("kpiindex = 'k1' ")
      .select("kpiquery")
      .first()
      .get(0)
      .toString
    qTable = queryDS
      .where("kpiindex = 'k1' ")
      .select("kpitable")
      .first()
      .get(0)
      .toString
    println(
      Insight
        .writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable)
        .get
    )

    println("KPI2a: Rank the SKUs based on the revenue - Worldwide")
    println("----------------------------------------------------")
    qString = queryDS
      .where("kpiindex = 'k2a' ")
      .select("kpiquery")
      .first()
      .get(0)
      .toString
    qTable = queryDS
      .where("kpiindex = 'k2a' ")
      .select("kpitable")
      .first()
      .get(0)
      .toString

    println(
      Insight
        .writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable)
        .getOrElse("Failed in KPII2a")
    )

    qString = queryDS
      .where("kpiindex = 'k2b' ")
      .select("kpiquery")
      .first()
      .get(0)
      .toString
    qTable = queryDS
      .where("kpiindex = 'k2b' ")
      .select("kpitable")
      .first()
      .get(0)
      .toString

    println(
      "KPI2b:" + qTable + ":Rank the SKUs based on the revenue - Countrywide"
    )
    println("------------------------------------------------------")
    println(
      Insight
        .writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable)
        .getOrElse("Failed in KPII2b")
    )
    println(
      "KPI3:" + qTable + ":Identify Sales Anomalies in any month for a given SKU"
    )
    println("-----------------------------------------------------------")
    qString = queryDS
      .where("kpiindex = 'k3' ")
      .select("kpiquery")
      .first()
      .get(0)
      .toString
    qTable = queryDS
      .where("kpiindex = 'k3' ")
      .select("kpitable")
      .first()
      .get(0)
      .toString

    println(
      "KPI3:" + qTable + ":Identify Sales Anomalies in any month for a given SKU"
    )
    println("-----------------------------------------------------------")
    println(
      Insight
        .writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable)
        .getOrElse("Failed in KPI3")
    )

    qString = queryDS
      .where("kpiindex = 'k4' ")
      .select("kpiquery")
      .first()
      .get(0)
      .toString
    qTable = queryDS
      .where("kpiindex = 'k4' ")
      .select("kpitable")
      .first()
      .get(0)
      .toString

    println(
      "KPI4:" + qTable + ":Rank the most valuable to least valuable customers"
    )
    println("--------------------------------------------------------")
    println(
      Insight
        .writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable)
        .getOrElse("Failed in KPI4")
    )

    qString = queryDS
      .where("kpiindex = 'k5' ")
      .select("kpiquery")
      .first()
      .get(0)
      .toString
    qTable = queryDS
      .where("kpiindex = 'k5' ")
      .select("kpitable")
      .first()
      .get(0)
      .toString

    println(
      "KPI5:" + qTable + ":Rank the highest revenue to lowest revenue generating countries"
    )
    println("--------------------------------------------------------")
    println(
      Insight
        .writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable)
        .getOrElse("Failed in KPI5")
    )

    qString = queryDS
      .where("kpiindex = 'k6' ")
      .select("kpiquery")
      .first()
      .get(0)
      .toString
    qTable = queryDS
      .where("kpiindex = 'k6' ")
      .select("kpitable")
      .first()
      .get(0)
      .toString

    println("KPI6:" + qTable + ":Revenue per SKU - Quarterwise")
    println("-----------------------------------")
    println(
      Insight
        .writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable)
        .getOrElse("Failed in KPI6")
    )

    qString = queryDS
      .where("kpiindex = 'k7' ")
      .select("kpiquery")
      .first()
      .get(0)
      .toString
    qTable = queryDS
      .where("kpiindex = 'k7' ")
      .select("kpitable")
      .first()
      .get(0)
      .toString

    println("KPI7:" + qTable + ": Revenue per country - QTR")
    println("-------------------------------")
    println(
      Insight
        .writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable)
        .getOrElse("Failed in KPI7")
    )

  }
}
