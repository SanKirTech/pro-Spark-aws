#!/bin/bash
# Summary Tables load scripts
gsutil cat gs://sankir-storage-prospark/kpi/load_summary/k1a_summary.sql  | bq query --use_legacy_sql=false
gsutil cat gs://sankir-storage-prospark/kpi/load_summary/k1b_summary.sql  | bq query --use_legacy_sql=false
gsutil cat gs://sankir-storage-prospark/kpi/load_summary/k2_summary.sql  | bq query --use_legacy_sql=false
gsutil cat gs://sankir-storage-prospark/kpi/load_summary/k3_summary.sql  | bq query --use_legacy_sql=false
gsutil cat gs://sankir-storage-prospark/kpi/load_summary/k4_summary.sql  | bq query --use_legacy_sql=false
gsutil cat gs://sankir-storage-prospark/kpi/load_summary/k5_summary.sql  | bq query --use_legacy_sql=false
gsutil cat gs://sankir-storage-prospark/kpi/load_summary/k6_summary.sql  | bq query --use_legacy_sql=false
gsutil cat gs://sankir-storage-prospark/kpi/load_summary/k7_summary.sql  | bq query --use_legacy_sql=false