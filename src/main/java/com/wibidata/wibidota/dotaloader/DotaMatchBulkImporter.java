/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wibidata.wibidota.dotaloader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.wibidata.wibidota.dotaloader.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;
/**
 * Bulk-importer to load the information about Dota 2 matches
 *
 * <p>Input files will contain JSON data representing a single match. The JSON
 * is expected to follow the API found at http://dev.dota2.com/showthread.php?t=58317.
 * with the following exceptions:
 * - AccountId
 *
 * <pre>
 * { "user_id" : "0", "play_time" : "1325725200000", "song_id" : "1" }
 * </pre>
 *
 * The bulk-importer expects a text input format:
 *   <li> input keys are the positions (in bytes) of each line in input file;
 *   <li> input values are the lines, as Text instances.
 */
public class DotaMatchBulkImporter extends KijiBulkImporter<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(DotaMatchBulkImporter.class);
  /** {@inheritDoc} */

  // RunetimeException to use if JSONReader does not read what it expects from
  // a JSONObject
  private static class BadReadException extends RuntimeException {
    public BadReadException(){
      super();
    }
    public BadReadException(String msg){
      super(msg);
    }
    public BadReadException(Exception ex){
      super(ex);
    }
  }


  private static String safeToString(Object o){
    return (o == null ? "null" : o.toString());
  }

  // Convenience class to read typed data from a String-Object map in a typed
  // way. Assumes caller knows the correct type for each key
  private static class JSONReader {

    Map<String, Object> obj;

    public JSONReader(Map<String, Object> obj){
      this.obj = obj;
    }

    private static String genInfoLossMsg(String key, Object valueUsed, Object realValue){
      return String.format("For key %s information was lost (using %s but key was %s)",
            key, valueUsed.toString(), realValue.toString());
    }

    private static void checkNull(String key, Object o){
      if(o == null){
        throw new BadReadException(key + " was null when it was not supposed to be!");
      }
    }

    public static BadReadException nullRead(String key){
      return new BadReadException(key + " was null when it was not supposed to be!");
    }

    /*
    public <T> T read(String key){
      Object o = obj.get(key);
      if(o == null){
        throw new BadReadException(key + " was null when it was not supposed to be!");
      }
      return validateObject(key, o);
    }

    public <T> T read(String key, T def){
      Object o = obj.get(key);
      if(o == null){
        return def;
      }
      return validateObject(key, o);
    }

    public <T> T validateObject(String key, Object o){
      try {
        switch T.

        }
        return (T) o;
      } catch (ClassCastException ex){
        return new BadReadException("Bad ")
      }
    }             */

    public Integer readInt(String key){
      Object o = obj.get(key);
      checkNull(key, o);
      return validateInt(key, o, true);


    }
    public Integer readInt(String key, Integer def){
      Object o = obj.get(key);
      if(o == null){
        return def;
      }
      return validateInt(key, o, true);
    }

    private Integer validateInt(String key, Object o, boolean ignoreMax){
      Number n = (Number) o;
      int out = n.intValue();
      if(n.longValue() != out) {
        if(!(ignoreMax && n.longValue() == 4294967295L)){
          throw new BadReadException(genInfoLossMsg(key, out, n.longValue()));
        }
      };
      if(n.doubleValue()%1.0 != 0.0) {
        throw new BadReadException(genInfoLossMsg(key, out, n.doubleValue()));
      };
      return out;
    }

    public Boolean readBool(String key){
      return readBool(key, false);
    }

    public Boolean readBool(String key, boolean allowNull){
      Object o = obj.get(key);
      if(o == null){
        if(allowNull){
          return null;
        } else {
          throw nullRead(key);
        }
      }
      return (Boolean) o;
    }

    public Long readLong(String key){
      Object o = obj.get(key);
      checkNull(key, o);
      return validateLong(key, o);


    }
    public Long readLong(String key, Long def){
      Object o = obj.get(key);
      if(o == null){
        return def;
      }
      return validateLong(key, o);
    }

    public Long validateLong(String key, Object o){
      Number n = (Number) 0;
      long out = n.longValue();
      if(n.doubleValue()%1.0 != 0.0){
        throw new BadReadException(genInfoLossMsg(key, out, n.doubleValue()));
      }
      return out;
    }

    public Double readDouble(String key){
      return readDouble(key, false);
    }

    public Double readDouble(String key, boolean allowNull){
      Object o = obj.get(key);
      if(o == null){
        if(allowNull){
          return null;
        } else {
          throw nullRead(key);
        }
      }
      return (((Number) o).doubleValue());
    }

    public List<Object> readArray(String key){
      return readArray(key, false);
    }

    public List<Object> readArray(String key, boolean allowNull){
      Object o = obj.get(key);
      if(o == null){
        if(allowNull){
          return null;
        } else {
          throw nullRead(key);
        }
      }
      return (List<Object>) o;
    }

    public String readString(String key){
      return readString(key, false);
    }

    public String readString(String key, boolean allowNull){
      Object o = obj.get(key);
      if(o == null){
        if(allowNull){
          return null;
        } else {
          throw nullRead(key);
        }
      }
      return (String) o;
    }

    public Map<String, Object> readObject(String key){
      return readObject(key, false);
    }

    public Map<String, Object> readObject(String key, boolean allowNull){
      Object o = obj.get(key);
      if(o == null){
        if(allowNull){
          return null;
        } else {
          throw nullRead(key);
        }
      }
      return (Map<String, Object>) o;
    }
  }

  // Reads an AbilityUpgrade object from a Map of its fields
  private AbilityUpgrade extractAbility(Map<String, Object> abilityData){
    final JSONReader reader = new JSONReader(abilityData);
    return AbilityUpgrade.newBuilder()
            .setLevel(reader.readInt("level"))
            .setAbilityId(reader.readInt("ability"))
            .setTime(reader.readInt("time"))
            .build();
  }

  // Reads a list of item_ids from a JSON reader, assumes
  // the items are encoded as item_0, item_1, ... item_5
  private List<Integer> readItems(JSONReader reader){
    final List<Integer> itemIds = new ArrayList<Integer>(6);
    for(int i = 0; i < 6; i++){
      itemIds.add(reader.readInt("item_" + i));
    }
    return itemIds;
  }

  // Reads a Player Object from a map of its fields->values
  private Player extractPlayer(Map<String, Object> playerData){
    final JSONReader reader = new JSONReader(playerData);
    Player.Builder builder = Player.newBuilder();

    // Set the abilityUpgrades                       /home/chris/kiji/wibidota-loader/lib/json-simple-1.1.jar
    final List<AbilityUpgrade> abilityUpgrades = new ArrayList<AbilityUpgrade>();

    final List<Object> uncastAbilities = reader.readArray("ability_upgrades", true);
    // This can be null (players have no abilities?)
    if(uncastAbilities != null){
      for(Object o : uncastAbilities){
        abilityUpgrades.add(extractAbility((Map<String, Object>) o));
      }
    }
    builder.setAbilityUpgrades(abilityUpgrades);

    // Set the additionalUnit
    Map<String, Object> additionalUnit = reader.readObject("additional_units", true);
    if(additionalUnit != null){
      final JSONReader unitReader = new JSONReader(additionalUnit);
      builder.setAdditionalUnits(
          AdditionalUnit.newBuilder()
              .setName(reader.readString("unitname"))
              .setItemIds(readItems(reader))
              .build());
    } else {
      builder.setAdditionalUnits(null);
    }

    // Set everything else
    return builder
             .setAccountId(reader.readLong("account_id", -1L))
             .setAssists(reader.readInt("assists"))
             .setDeaths(reader.readInt("deaths"))
             .setDenies(reader.readInt("denies"))
             .setExpPerMinute(reader.readInt("xp_per_min"))
             .setHeroId(reader.readInt("hero_id"))
             .setLastHits(reader.readInt("last_hits"))
             .setLeaverStatus(reader.readInt("leaver_status", -1))
             .setLevel(reader.readInt("level"))
             .setPlayerSlot(reader.readInt("player_slot"))
             .setTowerDamage(reader.readInt("tower_damage"))
             .setGoldSpent(reader.readInt("gold_spent"))
             .setGold(reader.readInt("gold"))
             .setGoldPerMinute(reader.readDouble("gold_per_min"))
             .setHeroDamage(reader.readInt("hero_damage"))
             .setHeroHealing(reader.readInt("hero_healing"))
             .setKills(reader.readInt("kills"))
             .setItemIds(readItems(reader))
             .build();
  }


  @Override
  public void produce(LongWritable filePos, Text line, KijiTableContext context)
      throws IOException {

    final JSONReader reader;
    try {
      final JSONParser parser = new JSONParser();
      reader = new JSONReader((JSONObject) parser.parse(line.toString()));
    } catch (ParseException pe){
      LOG.error("Failed to parse JSON record '{}' {}", line, pe);
      return;
    }
    try {
      final int gameMode = reader.readInt("game_mode");
      final int lobbyType = reader.readInt("lobby_type");
      final long matchId = reader.readLong("match_id");
      final int direTowers = reader.readInt("tower_status_dire");
      final int radiantTowers = reader.readInt("tower_status_radiant");
      final int direBarracks = reader.readInt("barracks_status_dire");
      final int radiantBarracks = reader.readInt("barracks_status_radiant");
      final int cluster = reader.readInt("cluster");
      final int season = reader.readInt("season");
      final long startTime = reader.readLong("start_time");
      final int seqNum = reader.readInt("match_seq_num");
      final int leagueId = reader.readInt("leagueid");
      final int firstBloodTime = reader.readInt("first_blood_time");
      final int negativeVotes = reader.readInt("negative_votes");
      final int positiveVotes = reader.readInt("positive_votes");
      final int duration = reader.readInt("duration");
      final boolean radiantWin = reader.readBool("radiant_win");

      final List<Player> playerStats = new ArrayList<Player>(10);
      for(Object o : reader.readArray("players")){
        playerStats.add(extractPlayer((Map<String, Object>) o));
      }
      final Players players = Players.newBuilder().setPlayers(playerStats).build();

      if(lobbyType < -1 || lobbyType > 5){
        throw new RuntimeException("BAD LOBBY TYPE");
      }

      EntityId eid = context.getEntityId(matchId + "");
      context.put(eid, "data", "match_id",  startTime, matchId);
      context.put(eid, "data", "dire_towers_status", startTime, direTowers);
      context.put(eid, "data", "radiant_towers_status", startTime, radiantTowers);
      context.put(eid, "data", "dire_barracks_status", startTime, direBarracks);
      context.put(eid, "data", "radiant_barracks_status", startTime, radiantBarracks);
      context.put(eid, "data", "cluster", startTime, cluster);
      context.put(eid, "data", "season", startTime, season);
      context.put(eid, "data", "start_time", startTime, startTime);
      context.put(eid, "data", "match_seq_num", startTime, seqNum);
      context.put(eid, "data", "league_id", startTime, leagueId);
      context.put(eid, "data", "first_blood_time", startTime, firstBloodTime);
      context.put(eid, "data", "negative_votes", startTime, negativeVotes);
      context.put(eid, "data", "positive_votes", startTime, positiveVotes);
      context.put(eid, "data", "duration", startTime, duration);
      context.put(eid, "data", "radiant_wins", startTime, radiantWin);
      context.put(eid, "data", "player_data", startTime, players);
      context.put(eid, "data", "game_mode", startTime, gameMode);
      context.put(eid, "data", "lobby_type", startTime, LobbyType.values()[lobbyType].toString());

    } catch (RuntimeException re){
      Long matchId = null;
      try {
        matchId = reader.readLong("match_id", null);
      } catch (RuntimeException ex){
      }
      try {
      LOG.error("Runtime Exception!!" + safeToString(matchId)
          + "\nLine\n" + safeToString(line) + "\nMessage:\n" + safeToString(re));
      } catch (RuntimeException ex) {
        LOG.error("Error loggging an error: " + ex.getMessage());
      }
      throw re;
    }
  }
}
