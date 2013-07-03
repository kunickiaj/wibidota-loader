
MODULE bulkimport;
LOAD DATA INFILE 'hdfs://shared/dota_matches'
INTO TABLE dota_matches
THROUGH PATH 'hdfs://shared/dota_tmp'
USING com.wibidata.wibidota.dotaloader.DotaMatchBulkImporter
;