Main class or Jar: com.sankir.smp.core.ApplicationMain

Jar file - gs://sankir-storage-prospark/jar/prospark-1.0-SNAPSHOT.jar
Note: Change this to where you uploaded the jar

```
Arguments

--projectId=pro-Spark
--schemaLocation=gs://sankir-storage-prospark/input/t_transaction.json
--inputLocation=gs://sankir-storage-prospark/processed-retail-data/q1/2011-01-05.json
--bqDataset=retail_bq
--bqTableName=t_transaction
--bqErrorTable=t_error
--kpiLocation=gs://sankir-storage-prospark/kpi/kpiquery.json

Note: Change the storage bucket (sankir-storage-prospark) to the bucket created by you

```