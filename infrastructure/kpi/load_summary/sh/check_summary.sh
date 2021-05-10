#!/bin/bash

echo "SUMMARY TABLE OUTPUT" > summary_results.txt
echo "---------------------" >> summary_results.txt

echo "pro-spark.retail_kpi_summary.t_customer_rank_summary" >> summary_results.txt
bq query --use_legacy_sql=false 'SELECT * FROM pro-spark.retail_kpi_summary.t_customer_rank_summary order by rank asc LIMIT 100' >> summary_results.txt

echo "pro-spark.retail_kpi_summary.t_revenue_country_summary" >> summary_results.txt
bq query --use_legacy_sql=false 'SELECT * FROM pro-spark.retail_kpi_summary.t_revenue_country_summary order by rank asc LIMIT 100' >> summary_results.txt
echo "" >> summary_results.txt

echo "pro-spark.retail_kpi_summary.t_revenue_country_qtr_summary" >> summary_results.txt
bq query --use_legacy_sql=false 'SELECT * FROM pro-spark.retail_kpi_summary.t_revenue_country_qtr_summary order by revenue desc LIMIT 100' >> summary_results.txt
echo "" >> summary_results.txt

echo "pro-spark.retail_kpi_summary.t_revenue_qtr_summary" >> summary_results.txt
echo "" >> summary_results.txt
bq query --use_legacy_sql=false 'SELECT * FROM pro-spark.retail_kpi_summary.t_revenue_qtr_summary LIMIT 100' >> summary_results.txt

echo "pro-spark.retail_kpi_summary.t_sales_anomaly_summary" >> summary_results.txt
bq query --use_legacy_sql=false 'SELECT * FROM pro-spark.retail_kpi_summary.t_sales_anomaly_summary LIMIT 100' >> summary_results.txt
echo "" >> summary_results.txt

echo "pro-spark.retail_kpi_summary.t_sku_dow_summary" >> summary_results.txt
bq query --use_legacy_sql=false 'SELECT * FROM pro-spark.retail_kpi_summary.t_sku_dow_summary LIMIT 100' >> summary_results.txt
echo "" >> summary_results.txt

echo "pro-spark.retail_kpi_summary.t_sku_revenue_c_summary" >> summary_results.txt
bq query --use_legacy_sql=false 'SELECT * FROM pro-spark.retail_kpi_summary.t_sku_revenue_c_summary LIMIT 100'  >> summary_results.txt
echo "" >> summary_results.txt

echo "pro-spark.retail_kpi_summary.t_sku_revenue_w_summary" >> summary_results.txt
bq query --use_legacy_sql=false 'SELECT * FROM pro-spark.retail_kpi_summary.t_sku_revenue_w_summary LIMIT 100' >> summary_results.txt
echo "" >> summary_results.txt
