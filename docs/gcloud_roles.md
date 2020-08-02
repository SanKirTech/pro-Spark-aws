## The below commands are one time command
### Create service account
`gcloud iam service-accounts create pro-spark-explorer --description="Pro Spark Explorer" --display-name="ProSpark Explorer"`
### Create service account Key 
`gcloud iam service-accounts keys create --iam-account pro-spark-explorer@sankir-1705.iam.gserviceaccount.com key.json`

### List Keys
`gcloud iam service-accounts keys list --iam-account pro-spark-explorer@sankir-1705.iam.gserviceaccount.com`

### Add roles
```ssh
gcloud projects add-iam-policy-binding sankir-1705 --member=serviceAccount:pro-spark-explorer@sankir-1705.iam.gserviceaccount.com --role=roles/viewer
gcloud projects add-iam-policy-binding sankir-1705 --member=serviceAccount:pro-spark-explorer@sankir-1705.iam.gserviceaccount.com --role=roles/bigquery.dataOwner
gcloud projects add-iam-policy-binding sankir-1705 --member=serviceAccount:pro-spark-explorer@sankir-1705.iam.gserviceaccount.com --role=roles/compute.instanceAdmin
gcloud projects add-iam-policy-binding sankir-1705 --member=serviceAccount:pro-spark-explorer@sankir-1705.iam.gserviceaccount.com --role=roles/storage.admin
gcloud projects add-iam-policy-binding sankir-1705 --member=serviceAccount:pro-spark-explorer@sankir-1705.iam.gserviceaccount.com --role=roles/pubsub.editor
gcloud projects add-iam-policy-binding sankir-1705 --member=serviceAccount:pro-spark-explorer@sankir-1705.iam.gserviceaccount.com --role=roles/dataproc.editor
```