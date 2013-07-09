CREATE TABLE dota_matches
ROW KEY FORMAT HASH PREFIXED(1)
WITH LOCALITY GROUP match_data (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH NONE,
  FAMILY data (
          match_id "long",
          dire_towers_status "int",
          radiant_towers_status "int",
          dire_barracks_status "int",
          radiant_barracks_status "int",
          cluster "int",
          season ["null","int"],
          start_time "long",
          game_mode "int",
          match_seq_num "long",
          league_id "int",
          first_blood_time "int",
          negative_votes "int",
          duration "int",
          radiant_wins "boolean",
          positive_votes "int",
          lobby_type ["null", "int"],
          player_data CLASS com.wibidata.wibidota.avro.Players
  ),
  MAP TYPE FAMILY derived_data "double"
);
