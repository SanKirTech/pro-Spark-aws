#!/bin/sh

bucket="s3://retail-sankir"
touch _empty

echo "------"
echo "Creating folders in bucket $bucket...."

aws s3 cp ./_empty $bucket/errors/
aws s3 cp ./_empty $bucket/ingress/
# s3 cp ./_empty $bucket/jar/
aws s3 cp ./_empty $bucket/kpi/
aws s3 cp ./_empty $bucket/processed-retail-data/
aws s3 cp ./_empty $bucket/recon/
aws s3 cp ./_empty $bucket/retail_kpi/t_customer_rank_dly/
aws s3 cp ./_empty $bucket/retail_kpi/t_revenue_country_dly/
aws s3 cp ./_empty $bucket/retail_kpi/t_sku_revenue_c_dly/
aws s3 cp ./_empty $bucket/retail_kpi/t_sku_revenue_w_dly/
aws s3 cp ./_empty $bucket/schemas/
#aws s3 cp ./_empty $bucket/security/