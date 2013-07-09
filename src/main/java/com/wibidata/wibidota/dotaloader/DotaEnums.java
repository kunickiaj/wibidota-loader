package com.wibidata.wibidota.dotaloader;

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
 * Class that can translate the raw values held in the dota_matches tables to
 * easier to use enums
 */
public final class DotaEnums {

  private DotaEnums() {}

  public static enum GameMode {
    UNKNOWN_ZERO, ALL_PICK, CAPTAINS_MODE, RANDOM_DRAFT, SINGLE_DRAFT, ALL_RANDOM,
    UNKOWN_SIX, THE_DIRETIDE, REVERSE_CAPTAINS_MODE, GREEVILING,
    TUTORIAL, MID_ONLY, LEAST_PLAYED, NEW_PLAYER_POOL, COMPENDIUM;

    public static GameMode fromInt(int i){
      return GameMode.values()[i];
    }
  }

  public static enum LobbyType {
    INVALID, PUBLIC_MATCHMAKING, PRACTICE, TOURNAMENT, TUTORIAL, CO_OP_WITH_BOTS, TEAM_MATCH, SOLO_QUEUE;

    public static LobbyType fromInt(int i){
      return LobbyType.values()[i + 1];
    }
  }

  public static enum LeaverStatus {
    STAYED, SAFE_ABANDON, DISCONNECT_ABANDON, ABANDON, ABANDON_AND_RECONNECT,
    UNKOWN_FIVE, UNKNOWN_SIX, BOT;
  }

  public static LeaverStatus fromInt(Integer i){
    return (i == null ? LeaverStatus.BOT : LeaverStatus.values()[i]);
  }
}
