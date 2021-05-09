#!/bin/bash


echo "#!/bin/bash" > $HOME/load_summary.sh
echo "# Summary Tables load scripts" >> $HOME/load_summary.sh

#for i in `gsutil ls gs://sankir-storage-prospark/kpi/load_summary/*.sql`
for i in `gsutil ls $1`
do 

 echo "gsutil cat $i  | bq query --use_legacy_sql=false" >> $HOME/load_summary.sh

done
chmod +x $HOME/load_summary.sh

echo "Summary scripts are generated in $HOME/load_summary.sh"