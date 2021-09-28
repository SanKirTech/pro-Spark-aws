/*
 *
 *  * SanKir Technologies
 *  * (c) Copyright 2021.  All rights reserved.
 *  * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  *  without the prior written permission from Sankir Technologies.
 *  *
 *  * The course contents can be accessed by subscribing to pro-Spark course.
 *  *
 *  * Please visit www.sankir.com for details.
 *  *
 *
 */

package com.sankir.smp.core.transformations

import com.sankir.smp.cloud.common.CloudConnector
import com.sankir.smp.utils.LogFormatter.formatHeader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}

object Insight extends Logging {
  private var cloudConnector : CloudConnector = _
  def initialize(cloudConnector: CloudConnector)  {
    this.cloudConnector = cloudConnector
  }

  /**
   * <h2>kpiQuery.json dictionary</h2>
   * <pre>
   * {
   *      "kpiindex":"k1",
   *      "kpitable":"retail_kpi.t_sku_dow_dly",
   *      "kpiquery":
   *        "select distinct stockcode,
   *        sum(round(quantity * unitprice)) ov0er w as revenue,
   *        dayofweek(InvoiceDate)
   *        as Day_Of_Week, country from %s window w as (partition by stockcode,
   *        country order by dayofweek(InvoiceDate),country) order by revenue desc,
   *        Day_Of_Week,country"
   * }
   * </pre>
   */

  private case class kpiSchema(kpiindex: String, kpitable: String, kpiquery: String)

  /**
    * method name: runKPIQuery
    * @param sparkSession - instance of the Spark Session
    * @param sparkTable - spark table in global_temp schema
    * @param kpiLocation - Location of the kpi json file
    * @return unit - does not return anything
    */

  def runKPIQuery(sparkSession: SparkSession,
                  sparkTable: String,
                  kpiLocation: String): Unit = {

    logInfo("KPI Location in GCP : " + kpiLocation)
    var qString: String = "" //Query String to be populated
    var qTable: String = "" //table to be used for storing the results
    var qDisc: String = "" //KPI Description

    import sparkSession.implicits._
    val queryDS = sparkSession.read.json(kpiLocation).as[kpiSchema]


    val kpiIndices = queryDS
      .select("kpiindex").as[String].collect()

    /**
      *
      * @param kpiIndex index position in kpiquery to fetch the right sql script
      */
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

      logInfo(formatHeader(kpiIndex + ":" + qTable + ":" + qDisc))
      logInfo("Query String")
      logInfo(qString)
      val kpiDF = sparkSession.sql(qString.format(sparkTable))
      this.cloudConnector.saveKPI(kpiDF,qTable)
    }

    kpiIndices.foreach(kpiPrint)

  }

}
