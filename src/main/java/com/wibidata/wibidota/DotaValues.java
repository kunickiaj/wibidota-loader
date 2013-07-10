package com.wibidata.wibidota;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;

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

/*
 * Class that can represent raw values held in the dota_matches tables in a more human readable
 * way. NOTE this should be relatively stable but changes might occur if Valve changes their
 * API. In particular additional enum may be added as more game modes or lobby types are
 * introduced.
 */
public final class DotaValues {


  // This class should not be instantiated
  private DotaValues() {}


  /**
   * Maps the play_slot int as returned by the Dota 2 api to
   * a more readable form
   * @param num, the player slot as encoded by the Dota 2 api
   * @return 0-4 for the first-last slots of the Radiant, 5-9 for
   * the first-last slots on the Dire
   */
  public static int playerSlot(int num){
    int slot = 0;
    if(num >> 7 == 1){
      slot += 5;
    }
    return slot + (num & 7);
  }

  public static class Towers {

    private final boolean[] towers;
    private short towersStanding;

    public Towers(int num){
      towersStanding = 0;
      towers = new boolean[11];
      for(int i = 0; i < 11; i++){
        towers[i] = 1 == (num & 1);
        if(towers[i]){
          towersStanding++;
        }
        num = num >> 1;
      }
    }

    public short getTowersStanding(){
      return towersStanding;
    }

    public boolean topT1(){
      return towers[0];
    }
    public boolean topT2(){
      return towers[1];
    }
    public boolean topT3(){
      return towers[2];
    }
    public boolean midT1(){
      return towers[3];
    }
    public boolean midT2(){
      return towers[4];
    }
    public boolean midT3(){
      return towers[5];
    }
    public boolean botT1(){
      return towers[6];
    }
    public boolean botT2(){
      return towers[7];
    }
    public boolean botT3(){
      return towers[8];
    }
    public boolean topAncient(){
      return towers[9];
    }
    public boolean botAncient(){
      return towers[10];
    }
  }

  public static class Barracks {

    private final boolean[] barracks;

    public Barracks(int num){
      barracks = new boolean[6];
      for(int i = 0; i < 6; i++){
        barracks[i] = 1 == (num & 1);
        num = num >> 1;
      }
    }

    public boolean topMelee() {
      return barracks[0];
    }
    public boolean topRanged(){
      return barracks[1];
    }
    public boolean midMelee(){
      return barracks[2];
    }
    public boolean midRanged(){
      return barracks[3];
    }
    public boolean botMelee(){
      return barracks[4];
    }
    public boolean botRanged(){
      return barracks[5];
    }
  }

  public static enum Columns {
    MATCH_ID("match_id"),
    DIRE_TOWERS_STATUS("dire_tower_staus"),
    RADIANT_TOWERS_STATUS("radiant_tower_status"),
    CLUSTER("cluster"),
    SEASON("season"),
    START_TIME("start_time"),
    GAME_MODE("game_mode"),
    MATCH_SEQ_NUM("match_seq_num"),
    LEAGUE_ID("league_id"),
    FIRST_BLOOD_TIME("first_blood_time"),
    NEGATIVE_VOTES("negative_votes"),
    DURATION("duration"),
    RADIANT_WIN("radiant_win"),
    POSITIVE_VOTES("positive_votes"),
    LOBBY_TYPE("lobby_type"),
    HUMAN_PLAYERS("human_players"),
    PLAYER_DATA("player_data");

    private final String name;
    Columns(String name) { this.name = name; }
    public String toString() { return name; }
    public KijiColumnName columnName() {
      return new KijiColumnName("data", name);}

  }

  // ***** enums used in Dota 2. ADDITIONAL ENUMS COULD BE ADDED AS DOTA 2 CHANGES *****

  public static enum GameMode {
    UNKNOWN_ZERO(0), ALL_PICK(1), CAPTAINS_MODE(2), RANDOM_DRAFT(3),
    SINGLE_DRAFT(4), ALL_RANDOM(5), UNKOWN_SIX(6), THE_DIRETIDE(7),
    REVERSE_CAPTAINS_MODE(8), GREEVILING(9), TUTORIAL(10), MID_ONLY(11),
    LEAST_PLAYED(12), NEW_PLAYER_POOL(13), COMPENDIUM(14);

    private final int rawValue;
    GameMode(int rawValue) { this.rawValue = rawValue; }
    public int getEncoding() { return rawValue; }

    public static GameMode fromInt(int i){
      return GameMode.values()[i];
    }
  }

  public static enum LobbyType {
    INVALID(-1), PUBLIC_MATCHMAKING(0), PRACTICE(1), TOURNAMENT(2), TUTORIAL(3),
    CO_OP_WITH_BOTS(4), TEAM_MATCH(5), SOLO_QUEUE(6);

    private final int rawValue;
    LobbyType(int rawValue) { this.rawValue = rawValue; }
    public int getEncoding() { return rawValue; }

    public static LobbyType fromInt(int i){
      return LobbyType.values()[i + 1];
    }

  }

  public static enum LeaverStatus {
    STAYED(0), SAFE_ABANDON(1), DISCONNECT_ABANDON(2), ABANDON(3), ABANDON_AND_RECONNECT(4),
    UNKOWN_FIVE(6), UNKNOWN_SIX(7), BOT(null);

    private final int rawValue;
    LeaverStatus(Integer rawValue) { this.rawValue = rawValue; }
    public Integer getEncoding() { return rawValue; }

    public static LeaverStatus fromInt(Integer i){
      return (i == null ? LeaverStatus.BOT : LeaverStatus.values()[i]);
    }
  }

  public static enum ItemSlot {
    TOP_LEFT(0), TOP_CENTER(1), TOP_RIGHT(2), BOT_RIGHT(3),
    BOT_CENTER(4), BOT_LEFT(5);


    private final int rawValue;
    ItemSlot(Integer rawValue) { this.rawValue = rawValue; }
    public Integer getEncoding() { return rawValue; }

    public static ItemSlot getItemSlot(int num){
      return ItemSlot.values()[num];
    }
  }
}
