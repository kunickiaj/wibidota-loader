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
package com.wibidota.wibiloader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.wibi.wibidota.dotaloader.*;
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
 * Bulk-importer to load the information about the track plays into the KijiMusic Users table.
 *
 * <p>Input files will contain JSON data representing track plays, with one song per line, as in:
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

  // Convenience class to read typed data from a String-Object map in a typed
  // way. Assumes caller knows the correct type for each key
  private static class JSONReader {

    Map<String, Object> obj;

    public JSONReader(Map<String, Object> obj){
      this.obj = obj;
    }

    public int readInt(String key){
      return (((Number) obj.get(key)).intValue());
    }

    public boolean readBool(String key){
      return (Boolean) obj.get(key);
    }

    public long readLong(String key){
      return (((Number) obj.get(key)).longValue());
    }

    public double readDouble(String key){
      return (((Number) obj.get(key)).doubleValue());
    }

    public List<Object> readArray(String key){
      return (List<Object>) obj.get(key);
    }

    public List<Integer> readIntArray(String key){
      // TODO: technically we can only assume List<Number> will work
      return (List<Integer>) obj.get(key);
    }

    public String readString(String key){
      return (String) obj.get(key);
    }

    public Map<String, Object> readObject(String key){
      return (Map<String, Object>) obj.get(key);
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

    // Set the abilityUpgrades
    final List<AbilityUpgrade> abilityUpgrades = new ArrayList<AbilityUpgrade>();
    for(Object o : reader.readArray("ability_upgrades")){
      abilityUpgrades.add(extractAbility((Map<String, Object>) o));
    }
    builder.setAbilityUpgrades(abilityUpgrades);

    // Set the additionalUnit
    Map<String, Object> additionalUnit = reader.readObject("additional_units");
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
             .setAccountId(reader.readInt("account_id"))
             .setAssists(reader.readInt("assists"))
             .setDeaths(reader.readInt("deaths"))
             .setDenies(reader.readInt("denies"))
             .setExpPerMinute(reader.readInt("xp_per_min"))
             .setHeroId(reader.readInt("hero_id"))
             .setLastHits(reader.readInt("last_hits"))
             .setLeaverStatus(LeaverStatus.values()[reader.readInt("leaver_status")])
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

    final JSONParser parser = new JSONParser();
    try {
      // Parse JSON:
      final JSONReader reader = new JSONReader((JSONObject) parser.parse(line.toString()));

      // TODO: maybe more robust to use a swtich statement?
      // The ENUM ordering and value align so we can use an offset
      final GameMode gameMode = GameMode.values()[reader.readInt("game_mode") + 1];

      // Again an offset works, watch this if the API changes
      final LobbyType lobbyType = LobbyType.values()[reader.readInt("lobby_type")];

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

      EntityId eid = context.getEntityId(matchId + "");
      context.put(eid, "data", "match_id", matchId);
      context.put(eid, "data", "dire_towers_status", direTowers);
      context.put(eid, "data", "radiant_towers_status", radiantTowers);
      context.put(eid, "data", "dire_barracks_status", direBarracks);
      context.put(eid, "data", "radiant_barracks_status", radiantBarracks);
      context.put(eid, "data", "cluster", cluster);
      context.put(eid, "data", "season", season);
      context.put(eid, "data", "start_time", startTime);
      context.put(eid, "data", "match_seq_num", seqNum);
      context.put(eid, "data", "league_id", leagueId);
      context.put(eid, "data", "first_blood_time", firstBloodTime);
      context.put(eid, "data", "negative_votes", negativeVotes);
      context.put(eid, "data", "positive_votes", positiveVotes);
      context.put(eid, "data", "duration", duration);
      context.put(eid, "data", "radiant_wins", radiantWin);
      context.put(eid, "data", "player_data", players);

    } catch (ParseException pe) {
      // Catch and log any malformed json records.
      // context.incrementCounter(KijiMusicCountears.JSONParseFailure);
      LOG.error("Failed to parse JSON record '{}' {}", line, pe);
    }
  }
}
