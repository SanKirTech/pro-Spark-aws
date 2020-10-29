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

package com.sankir.smp.pipelines.transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

object Insight {

  val retailSchema = StructType(Array(
    StructField("InvoiceNo", StringType, nullable = false),
    StructField("StockCode", StringType, nullable = false),
    StructField("Description",StringType, nullable = false),
    StructField("Quantity", IntegerType, nullable = false),
    StructField("InvoiceDate", DateType, nullable = false),
    StructField("UnitPrice", DoubleType, nullable = false),
    StructField("CustomerID", DoubleType, nullable = false),
    StructField("Country",StringType, nullable = false)))

  def writekpiToBigQ(sparkSession: SparkSession, kq: String, bqtbl: String): Option[String] = {
    val bucket = "sankir-storage-prospark"
    sparkSession.conf.set("temporaryGcsBucket", bucket)
    val kpiDF = sparkSession.sql(kq)
    kpiDF.printSchema()

    kpiDF.show(100,false)
   // kpiDF.write.format("bigquery")
    kpiDF.coalesce(5).write.format("bigquery")
      .mode("overwrite")
      .save(bqtbl)
    Some("Success")
  }

  def runKPIQuery(sparkSession: SparkSession, sparkTable: String, kpiLocation: String): Unit = {

    println("KPI Location in GCP : " + kpiLocation )
    var qString:String = "" //Query String to be populated
    var qTable:String = "" //BigQUery table to be used for storing the results
    val queryDF = sparkSession.read.json(kpiLocation)

    println("KPI1: Highest selling SKUs on a daily basis (M,T,W,Th,F,S,Su) per country")
    println("----------------------------------------------------")
    qString = queryDF.where("kpiindex = 'k1' ").select("kpiquery").first().get(0).toString
    qTable = queryDF.where("kpiindex = 'k1' ").select("kpitable").first().get(0).toString
    println(Insight.writekpiToBigQ(sparkSession, qString.format(sparkTable), qTable).get)

//    println("KPI2a: Rank the SKUs based on the revenue - Worldwide")
//    println("----------------------------------------------------")
//    qString = queryDF
//      .where("kpiindex = 'k2a' ")
//      .select("kpiquery")
//      .first().get(0).toString
//    qTable = queryDF
//      .where("kpiindex = 'k2a' ")
//      .select("kpitable")
//      .first().get(0).toString
//
//    println(
//      Insight.writekpiToBigQ
//      (
//        sparkSession,
//        qString.format(sparkTable),
//        qTable).
//        getOrElse("Failed in KPII2a")
//    )
//
//
//    qString = queryDF
//      .where("kpiindex = 'k2b' ")
//      .select("kpiquery")
//      .first().get(0).toString
//    qTable = queryDF
//      .where("kpiindex = 'k2b' ")
//      .select("kpitable")
//      .first().get(0).toString
//
//    println("KPI2b:" + qTable + ":Rank the SKUs based on the revenue - Countrywide")
//    println("------------------------------------------------------")
//    println(
//      Insight.writekpiToBigQ
//      (
//        sparkSession,
//        qString.format(sparkTable),
//        qTable).
//        getOrElse("Failed in KPII2b")
//    )
//    println("KPI3:" + qTable + ":Identify Sales Anomalies in any month for a given SKU")
//    println("-----------------------------------------------------------")
//    qString = queryDF
//      .where("kpiindex = 'k3' ")
//      .select("kpiquery")
//      .first().get(0).toString
//    qTable = queryDF
//      .where("kpiindex = 'k3' ")
//      .select("kpitable")
//      .first().get(0).toString
//
//    println("KPI3:" + qTable + ":Identify Sales Anomalies in any month for a given SKU")
//    println("-----------------------------------------------------------")
//    println(
//      Insight.writekpiToBigQ
//      (
//        sparkSession,
//        qString.format(sparkTable),
//        qTable).
//        getOrElse("Failed in KPI3")
//    )
//
//    qString = queryDF
//      .where("kpiindex = 'k4' ")
//      .select("kpiquery")
//      .first().get(0).toString
//    qTable = queryDF
//      .where("kpiindex = 'k4' ")
//      .select("kpitable")
//      .first().get(0).toString
//
//    println("KPI4:" + qTable + ":Rank the most valuable to least valuable customers")
//    println("--------------------------------------------------------")
//    println(
//      Insight.writekpiToBigQ
//      (
//        sparkSession,
//        qString.format(sparkTable),
//        qTable).
//        getOrElse("Failed in KPI4")
//    )
//
//    qString = queryDF
//      .where("kpiindex = 'k5' ")
//      .select("kpiquery")
//      .first().get(0).toString
//    qTable = queryDF
//      .where("kpiindex = 'k5' ")
//      .select("kpitable")
//      .first().get(0).toString
//
//    println("KPI5:" + qTable + ":Rank the highest revenue to lowest revenue generating countries")
//    println("--------------------------------------------------------")
//    println(
//      Insight.writekpiToBigQ
//      (
//        sparkSession,
//        qString.format(sparkTable),
//        qTable).
//        getOrElse("Failed in KPI5")
//    )
//
//    qString = queryDF
//      .where("kpiindex = 'k6' ")
//      .select("kpiquery")
//      .first().get(0).toString
//    qTable = queryDF
//      .where("kpiindex = 'k6' ")
//      .select("kpitable")
//      .first().get(0).toString
//
//    println("KPI6:" + qTable + ":Revenue per SKU - Quarterwise")
//    println("-----------------------------------")
//    println(
//      Insight.writekpiToBigQ
//      (
//        sparkSession,
//        qString.format(sparkTable),
//        qTable).
//        getOrElse("Failed in KPI6")
//    )
//
//    qString = queryDF
//      .where("kpiindex = 'k7' ")
//      .select("kpiquery")
//      .first().get(0).toString
//    qTable = queryDF
//      .where("kpiindex = 'k7' ")
//      .select("kpitable")
//      .first().get(0).toString
//
//    println("KPI7:" + qTable + ": Revenue per country - QTR")
//    println("-------------------------------")
//    println(
//      Insight.writekpiToBigQ
//      (
//        sparkSession,
//        qString.format(sparkTable),
//        qTable).
//        getOrElse("Failed in KPI7")
//    )

  }
}
