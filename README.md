Wibidota, Wibidata's collection and analysis of Dota 2 statistics

Building the Project
==========
Use mvn install to setup the project. Use mvn package to recompile it. Some of the classes require the gson library for parsing json text. The needed jar will be copied to the target/lib folder during the install phase. Be sure to add this lib to the KIJI_CLASSPATH enviromental variable.

Collecting And Importing the Data
===============
Data is collected through the script src/main/python/dota_slurp.py (see documentation for details). The script produced gziped files containing a json object of match data per a line. These files should be imported to hdfs. The data can then be ported into Kiji where it is stored using match_ids as row keys. Build the table by using:

''''
kiji-schema-shell --file=src/main/ddl/build_matches_table.ddl 
''''

The data can then be ported to this table using DotaMatchesBulkImporter.java through

''''
kiji bulk-import --importer=com.wibidata.wibidota.DotaMatchBulkImporter --input="format=text file=hdfs://path/to/matches/json" --output="format=hfile file=hdfs://path/to/tmp/file nsplits=<nsplits> table=kiji://.env/wibidota/dota_matches" --lib={WIBIDOTA_HOME}/lib
kiji bulk-load --table=kiji://.env/wibidota/dota_matches --hfile=hdfs://path/to/tmp/file
''''

An additional table exists that pivots the data onto a player centric model using account_ids (of non-anonoumys accounts) as row keys. This table can be built with

''''
kiji-schema-shell --file=src/main/ddl/build_player_table.ddl
''''

Data can be imported to this table using com.wibidata.wibidota.DotaPlayersBulkImporter from the raw json in the same manner as the dota_matches table. 

Utility Classes
============
Both the matches and players table store the data in a raw form as it was gathered from the Dota API. There is one notable exception, we store account_ids as 32bit signed ints where as Vavle stores them as 32bit unsigned ints. As a consequence some of our account_ids may be negative, however thus far this is only the case for account_ids that Vavle has set to 0xFFFFFFFF to indicate anonoumous accounts (in the table these accounts will have an id of -1). The utility class DotaValues.java contains methods for translating the raw data into more human readable form, including mapping the integer ids for abilities, items, and heroes into there respective names. It depends, and is only as accurate as, the json files in src/main/resources.



