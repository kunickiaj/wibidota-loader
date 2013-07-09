#!/bin/bash
arg=$1
if [ -z "$1" ]
  then
    echo "********** RECOMPILING **********"
    mvn package
fi
echo "********** DELETING TMP FILE  **********"
hadoop fs -rm -r hdfs://localhost:8020/tmp/dota_tmp
echo "********** IMPORTING  **********"
kiji bulk-import --importer=com.wibidata.wibidota.dotaloader.DotaMatchBulkImporter --input="format=text file=hdfs://localhost:8020/dota-matches/" --output="format=hfile file=hdfs://localhost:8020/tmp/dota_tmp nsplits=64 table=kiji://.env/default/dota_matches" --lib=/home/chris/kiji/wibidota-loader/lib