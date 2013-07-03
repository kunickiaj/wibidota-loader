MODULE bulkimport;
LOAD DATA INFILE 'hdfs://dota_matches'
INTO TABLE dota_matches
THROUGH PATH 'hdfs://dota_tmp'
USING com.wibidata.wibidota.dotaloader.DotaMatchBulkImporter
;