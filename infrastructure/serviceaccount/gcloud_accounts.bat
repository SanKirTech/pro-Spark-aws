@echo off
set project_name=pro-spark
set account_name=pro-spark-sa1
set key_location=E:\prospark
set key_file=%account_name%_cred.json

echo -----------------------------------------------------------------------------------
echo * SanKir Technologies
echo * (c) Copyright 2020.  All rights reserved.
echo * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted^
in any form or by any means including photocopying, recording or other electronic or mechanical methods,^
without the prior written permission from Sankir Technologies.
echo.
echo * The course contents can be accessed by subscribing to pro-Spark course.
echo * Please visit www.sankir.com for details.

echo -----------------------------------------------------------------------------------

echo setting project in gcloud config
call gcloud config set project %project_name%
echo -----------------------------------------------------------------------------------

echo Creating Service Account required for prospark project
echo Note the account is like an email address accountname@project-iam.gserviceaccount.com

call gcloud iam service-accounts create %account_name% --description="Pro Spark" --display-name="ProSpark"
echo -----------------------------------------------------------------------------------

echo Creating the key..
call gcloud iam service-accounts keys create --iam-account %account_name%@%project_name%.iam.gserviceaccount.com %key_location%\%key_file%
echo -----------------------------------------------------------------------------------
echo Listing the keys..
call gcloud iam service-accounts keys list --iam-account %account_name%@%project_name%.iam.gserviceaccount.com
echo -----------------------------------------------------------------------------------
echo Adding the following roles
echo BigQuery Admin
echo Compute Instance Admin (beta)
echo Dataproc Editor
echo Pub/Sub Editor
echo Storage Admin
echo Viewer
echo -----------------------------------------------------------------------------------
call gcloud projects add-iam-policy-binding %project_name% --member=serviceAccount:%account_name%@%project_name%.iam.gserviceaccount.com --role=roles/viewer
call gcloud projects add-iam-policy-binding %project_name% --member=serviceAccount:%account_name%@%project_name%.iam.gserviceaccount.com --role=roles/bigquery.dataOwner
call gcloud projects add-iam-policy-binding %project_name% --member=serviceAccount:%account_name%@%project_name%.iam.gserviceaccount.com --role=roles/compute.instanceAdmin
call gcloud projects add-iam-policy-binding %project_name% --member=serviceAccount:%account_name%@%project_name%.iam.gserviceaccount.com --role=roles/storage.admin
call gcloud projects add-iam-policy-binding %project_name% --member=serviceAccount:%account_name%@%project_name%.iam.gserviceaccount.com --role=roles/pubsub.editor
call gcloud projects add-iam-policy-binding %project_name% --member=serviceAccount:%account_name%@%project_name%.iam.gserviceaccount.com --role=roles/dataproc.editor
echo -----------------------------------------------------------------------------------

echo setting GOOGLE_APPLICATIONS_CREDENTIALS
setx GOOGLE_APPLICATION_CREDENTIALS %key_location%\%key_file%
echo GOOGLE_APPLICATION_CREDENTIALS is set to %GOOGLE_APPLICATION_CREDENTIALS%
echo -----------------------------------------------------------------------------------

echo Double check by logging into https://console.cloud.google.com/  and click on IAM ^& Admin and IAM. Refresh the browser ^if you don't see the changes
