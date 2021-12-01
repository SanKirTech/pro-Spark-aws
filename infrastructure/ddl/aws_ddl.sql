CREATE DATABASE retail_kpi;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_kpi.t_revenue_country_dly
(
  country string,
  revenue float,
  rank int)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION
  's3://retail-sankir/retail_kpi/t_revenue_country_dly';


CREATE EXTERNAL TABLE IF NOT EXISTS retail_kpi.t_customer_rank_dly
(
   customerid float,
   revenue float,
   rank int)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION
  's3://retail-sankir/retail_kpi/t_customer_rank_dly';


CREATE EXTERNAL TABLE IF NOT EXISTS retail_kpi.t_sku_revenue_c_dly
(
  `stockcode` string,
  `country` string,
  `revenue` float,
  `rank` int
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION
  's3://retail-sankir/retail_kpi/t_sku_revenue_c_dly';


CREATE EXTERNAL TABLE IF NOT EXISTS retail_kpi.t_sku_revenue_w_dly
(
  `stockcode` string,
  `revenue` float,
  `rank` int
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION
  's3://retail-sankir/retail_kpi/t_sku_revenue_w_dly';


  CREATE EXTERNAL TABLE IF NOT EXISTS errors.t_error
  (
    errorReplay STRING,
    timestamp TIMESTAMP,
    errorType STRING,
    payload STRING,
    stackTrace STRING,
    jobName STRING,
    errorMessage STRING
    )
    ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION
      's3://retail-sankir/errors/t_error';

   CREATE TABLE ingress.t_ingress IF NOT EXISTS
   (
     InvoiceNo STRING,
     InvoiceDate STRING,
     Quantity INT64,
     Description STRING,
     StockCode STRING,
     UnitPrice FLOAT64,
     CustomerID FLOAT64,
     Country STRING
     )
     ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
     STORED AS TEXTFILE
     LOCATION
        's3://retail-sankir/ingress/t_ingress';