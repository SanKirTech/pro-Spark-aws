package com.sankir.smp.pipelines

import com.sankir.smp.pipelines.validators.Validator.{jsonSchemaValidator, jsonStringValidator}
import com.sankir.smp.utils.ArgParser
import com.sankir.smp.utils.Resources.readAsStringFromGCS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType}


object ApplicationMain {
  def main(args: Array[String]): Unit = {

    //    Parsing the Arguments and updating the config object that will help us in providing necessary inputs
    val CMDLINEOPTIONS = ArgParser.parse(args)

    //Reading the schemaString
    val schema = readAsStringFromGCS(CMDLINEOPTIONS.projectId, CMDLINEOPTIONS.schemaLocation)

    // Creating SparkSession
    //val sparkSession = SparkSession.builder().appName("Pro-Spark-Batch").getOrCreate()
    val sparkSession = SparkSession.builder().appName("Pro-Spark-Batch").master("local[*]").getOrCreate()

    val JOBNAME = s"${sparkSession.sparkContext.appName}-${sparkSession.sparkContext.applicationId}"

    // Reading data from the input location
    import com.sankir.smp.utils.encoders.CustomEncoders._

    // sdfData is the dataset crated from the processedJSON with metadata
    val sdfData = sparkSession.read.textFile(CMDLINEOPTIONS.inputLocation)
    println("\n--------  sdfData ------------")
    sdfData.show(false)
    // jsonStringValidator validates whether the input data is valid or not
    val jsonValidatedRecords = jsonStringValidator(sdfData)

    // get the content wrapped in Success  rec_._2.get does this
    val jsonRecords = jsonValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    val inValidJsonRecords = jsonValidatedRecords.filter(_._2.isFailure)
    //writeToBigQuery(inValidJsonRecords, CMDLINEOPTIONS, JOBNAME, INVALID_JSON_ERROR)

    //    println("\n--------------- JSON validated records -------------")
    //    jsonValidatedRecords.collect().foreach(println)
    //
    //    println("\n--------------- valid JSON records ---------------")
    //    jsonRecords.collect().foreach(println)
    //
    //    println("\n--------------- invalid JSON records -------------")
    //inValidJsonRecords.collect().foreach(println)

    val schemaValidatedRecords = jsonSchemaValidator(jsonRecords, schema)
    val jsonRecordsWithProperSchema = schemaValidatedRecords.filter(_._2.isSuccess).map(rec => (rec._1, rec._2.get))
    val invalidSchemaRecords = schemaValidatedRecords.filter(_._2.isFailure)
    // writeToBigQuery(invalidSchemaRecords, CMDLINEOPTIONS, JOBNAME, SCHEMA_VALIDATION_ERROR)

    //    println("\n---------------- schema validated records ------")
    //    schemaValidatedRecords.collect().foreach(println)
    //
    //
    //    println("\n---------------- valid Schema records ------")
    //    jsonRecordsWithProperSchema.collect().foreach(println)
    //
    //
    //    println("\n---------------- invalid Schema records ------")
    //    invalidSchemaRecords.collect().foreach(println)

    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    // StructType, StructField, StringType, IntegerType, DoublleType are sql Datatypes
    // println("\n---------------- kpi1 Dataframe------")
    val retailSchema = StructType(Array(
      StructField("InvoiceNo", StringType, nullable = true),
      StructField("StockCode", StringType, nullable = true),
      StructField("Description", StringType, nullable = true),
      StructField("Quantity", IntegerType, nullable = true),
      StructField("InvoiceDate", DateType, nullable = true),
      StructField("UnitPrice", DoubleType, nullable = true),
      StructField("CustomerID", StringType, nullable = true),
      StructField("Country", StringType, nullable = true)))
    //val kpi1 = sparkSession.read.json(jsonRecordsWithProperSchema.map(_._2.toString))
    val kpi1 = sparkSession.read.schema(retailSchema).json(jsonRecordsWithProperSchema.map(_._2.toString))
//    val kpi1 = sparkSession.read.schema(retailSchema)
//      .json(jsonRecordsWithProperSchema
//        .map(rec => JsonTransformation.convertJsonNodesToProperFormat(rec._2).toString))

    kpi1.printSchema()
     kpi1.show    // Dataframe output

    // Now view Datframe data through spark.sql
    println("\n----------------Spark sql table retial_tbl------")
    kpi1.createOrReplaceGlobalTempView("retail_tbl")
    sparkSession.sql("SELECT * from global_temp.retail_tbl").show(false)

    println("\n----------------Revenue per stockcode------")
    sparkSession.sql("SELECT distinct stockcode,  quantity*unitprice as Revenue from global_temp.retail_tbl").show(false)

    //    sparkSession.sql("SELECT distinct stockcode,  (CAST(retail_tbl.`quantity` AS DOUBLE) * CAST(retail_tbl.`unitprice` AS DOUBLE)) as Revenue from global_temp.retail_tbl").show(false)
    //    sparkSession.sql("SELECT distinct stockcode,  (sum(round((CAST(retail_tbl.`quantity` AS DOUBLE) * CAST(retail_tbl.`unitprice` AS DOUBLE))))) from global_temp.retail_tbl").show(false)
    val bucket = "sankir-storage-prospark"
    sparkSession.conf.set("temporaryGcsBucket", bucket)

    println("KPI1: Highest selling SKUs on a daily basis (M,T,W,Th,F,S,Su) per country")
    val kpiDF1 = sparkSession.sql(
      """select distinct stockcode,  sum(round(quantity * unitprice)) over w as revenue,
                              dayofweek(InvoiceDate) as Day_Of_Week, country
                              from global_temp.retail_tbl
                              window w as (partition by stockcode,country order by dayofweek(InvoiceDate),country)
                              order by  revenue desc, Day_Of_Week,country""").coalesce(1)

    kpiDF1.write.format("bigquery")
      .mode("append")
      .save("retail_kpi.t_sku_dow_daily1")


    println("KPI2-1: Rank the SKUs based on the revenue - Worldwide")
    println("----------------------------------------------------")
    sparkSession.sql(
      """select stockcode, sum(quantity * unitprice) as revenue ,
                         rank() over ( order by sum(quantity * unitprice) desc ) as ranking
                         from global_temp.retail_tbl
                         group by stockcode""").show(false)

    println("KPI2-2: Rank the SKUs based on the revenue - Countrywide")
    println("------------------------------------------------------")
    sparkSession.sql(
      """select stockcode, country, sum(quantity * unitprice) as revenue ,
                       rank() over ( order by sum(quantity * unitprice) desc ) as ranking
                       from global_temp.retail_tbl
                       group by stockcode, country""").show(false)

    println("KPI3: Identify Sales Anomalies in any month for a given SKU")
    println("-----------------------------------------------------------")
    sparkSession.sql(
      """select stockcode, year(invoicedate) as Year, month(invoicedate)  as Month, sum(quantity * unitprice) as Revenue
                             from global_temp.retail_tbl
                             group by stockcode, year(invoicedate), month(invoicedate)
                             order by stockcode, year(invoicedate), month(invoicedate) """).show(10, false)

    println("KPI4: Rank the most valuable to least valuable customers")
    println("--------------------------------------------------------")
    sparkSession.sql(
      """select customerid, sum(quantity * unitprice) as revenue,
                             rank() over ( order by sum(quantity * unitprice) desc) as Most_valuable_rank
                             from global_temp.retail_tbl
                             group by customerid""").show(5, false)
    println("KPI5: Rank the highest revenue to lowest revenue generating countries")
    println("---------------------------------------------------------------------")
    sparkSession.sql(
      """select country, sum(quantity * unitprice),
                             rank() over ( order by sum(quantity * unitprice) desc) as Ranking
                             from global_temp.retail_tbl
                             group by country""").show(5, false)
    println("KPI6: Revenue per SKU - Quarterwise")
    println("-----------------------------------")
    sparkSession.sql(
      """select stockcode, year(invoicedate), quarter(invoicedate) , sum(quantity * unitprice) as revenue
                              from global_temp.retail_tbl
                              group by stockcode, year(invoicedate), quarter(invoicedate)
                              order by stockcode,year(invoicedate), quarter(invoicedate)""").show(5, false)
    println("KPI7: Revenue per country - QTR")
    println("-------------------------------")
    sparkSession.sql(
      """select country, year(invoicedate),
                             quarter(invoicedate) , sum(quantity * unitprice) as revenue
                             from global_temp.retail_tbl
                             group by country, year(invoicedate), quarter(invoicedate)
                             order by revenue desc""").show(5, false)

  }
}
