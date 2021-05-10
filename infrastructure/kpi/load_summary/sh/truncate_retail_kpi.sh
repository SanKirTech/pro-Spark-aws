#!/bin/bash

bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi.t_sku_dow_dly'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi.t_sku_dow_dly'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi.t_customer_rank_dly'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi.t_revenue_country_dly'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi.t_revenue_country_qtr_dly'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi.t_revenue_qtr_dly'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi.t_sales_anomaly_dly'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi.t_sku_revenue_c_dly'
bq query --use_legacy_sql=false 'truncate table pro-spark.retail_kpi.t_sku_revenue_w_dly'