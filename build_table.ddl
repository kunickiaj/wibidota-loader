CREATE TABLE dota_matches
ROW KEY FORMAT HASH PREFIXED(4)
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
          game_mode {"type" : "enum", "name" : "game_mode", "symbols" : [
                            "UNKNOWN_ZERO",
                            "ALL_PICK", "CAPTAINS_MODE", "RANDOM_DRAFT", 
                            "SINGLE_DRAFT", "ALL_RANDOM", "UNKOWN_SIX", "THE_DIRETIDE", 
                            "REVERSE_CAPTAINS_MODE", "GREEVILING", "TUTORIAL", 
                            "MID_ONLY", "LEAST_PLAYED", "NEW_PLAYER_POOL",
                            "COMPENDIUM"
                            ]
                     },
          match_seq_num "long",
          league_id "int",
          first_blood_time "int",
          negative_votes "int",
          duration "int",
          radiant_wins "boolean",
          positive_votes "int",
          lobby_type {"type" : "enum", "name" : "lobby_type", "symbols" : [ 
                             "INVALID", "PUBLIC_MATCHMAKING", "PRACTICE", "TOURNAMENT", 
                             "TUTORIAL", "CO_OP_WITH_BOTS", "TEAM_MATCH",
                             "SOLO_QUEUE"
                            ]
                     },
          player_data CLASS com.wibidata.wibidota.avro.Players
  ),
  MAP TYPE FAMILY derived_data "double"
);
