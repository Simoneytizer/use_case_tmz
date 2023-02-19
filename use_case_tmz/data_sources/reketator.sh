DIR_LOGS=logs

if [ -d "$DIR_LOGS" ];
then
    echo "$DIR_LOGS directory exists = change name before overwriting"
    exit 0
else
  mkdir logs
fi

for i in `seq 0 98` ; do
  echo "request for $i-th csv"
  # echo "https://reketor-ut5nyuuria-ew.a.run.app/query_gcs?bucket_name=wagonxmoneytizer&blob_name=$i.csv"
  curl "https://reketor-ut5nyuuria-ew.a.run.app/from_gcs_to_gcs?bucket_name=wagonxmoneytizer&blob_name=$i.csv" > logs/logs_$i.json </dev/null &>/dev/null &
done
