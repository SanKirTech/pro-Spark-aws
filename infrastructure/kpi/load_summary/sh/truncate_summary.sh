#!/bin/bash

bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi_summary.t_sku_dow_summary'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi_summary.t_sku_dow_summary'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi_summary.t_customer_rank_summary'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi_summary.t_revenue_country_summary'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi_summary.t_revenue_country_qtr_summary'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi_summary.t_revenue_qtr_summary'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi_summary.t_sales_anomaly_summary'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi_summary.t_sku_revenue_c_summary'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi_summary.t_sku_revenue_w_summary'
