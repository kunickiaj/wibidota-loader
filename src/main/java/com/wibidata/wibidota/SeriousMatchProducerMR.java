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

package com.wibidata.wibidota;

import com.wibidata.wibidota.avro.Players;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import com.wibidata.wibidota.DotaValues.Columns;
import com.wibidata.wibidota.DotaValues.LeaverStatus;
import com.wibidata.wibidota.DotaValues.LobbyType;
import com.wibidata.wibidota.avro.Player;
import org.kiji.schema.KijiURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Adds a 'serious_match' column to the derived data field that is 1.0 iff the match is a
 * a public mathmaking, tournament, team_match, solo_queue game played with game modes
 * AP, CP, AR, RD, LP, or Compendium and has no leavers. Timestamp is always 0.
 */
public class SeriousMatchProducerMR extends KijiProducer {

  private static final Logger LOG = LoggerFactory.getLogger(SeriousMatchProducer.class);

  static enum Counters {
    GOOD_MATCHES,  // Number of matches considered 'real'
    BAD_GAME_MODE, // Number of mathes with discounted due to game_mode
    BAD_LOBBY,     // Number of matches with a non-real lobby type
    LEAVERS,       // Number of mathches with leavers
    REAL_MATCH_WITH_LEAVERS, // Matches valid except for the presence of leavers
    BAD_MATCHES   // Number  of matches not considered real
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    return builder.addColumns(builder.newColumnsDef()
        .withMaxVersions(1)
        .add(Columns.GAME_MODE.columnName())
        .add(Columns.HUMAN_PLAYERS.columnName())
        .add(Columns.LOBBY_TYPE.columnName())
        .add(Columns.PLAYER_DATA.columnName())
    ).build();
  }

  @Override
  public String getOutputColumn() {
    return "derived_data";
  }

  @Override
  public void produce(KijiRowData kijiRowData, ProducerContext producerContext) throws IOException {
    boolean realMatch = true;

    // Check if this match passes
    int gameMode = (Integer) kijiRowData.getMostRecentCell("data", "game_mode").getData();
    if(!DotaValues.GameMode.seriousGame(DotaValues.GameMode.fromInt(gameMode))){
      producerContext.incrementCounter(Counters.BAD_GAME_MODE);
      realMatch = false;
    }
    Integer o = kijiRowData.getMostRecentValue("data", "lobby_type");
    LobbyType lobbyType = LobbyType.fromInt(o);
    if(!LobbyType.seriousLobby(lobbyType)){
      producerContext.incrementCounter(Counters.BAD_LOBBY);
      realMatch = false;
    }
    Players player_data = kijiRowData.getMostRecentValue("data", "player_data");
    List<Player> players = player_data.getPlayers();
    for(Player player : players){
      if(LeaverStatus.fromInt(player.getLeaverStatus()) != LeaverStatus.STAYED){
        producerContext.incrementCounter(Counters.LEAVERS);
        if(realMatch){
          producerContext.incrementCounter(Counters.REAL_MATCH_WITH_LEAVERS);
        }
        realMatch = false;
        break;
      }
    }

    // Write the result
    if(realMatch){
      producerContext.incrementCounter(Counters.GOOD_MATCHES);
      producerContext.put("serious_match", 0L, 1.0);
    } else {
      producerContext.incrementCounter(Counters.BAD_MATCHES);
    }
  }
}
