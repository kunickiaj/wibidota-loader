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

public class RealMatchProducer extends KijiProducer {

  private static final Logger LOG = LoggerFactory.getLogger(RealMatchProducer.class);

  static enum Counters {
    MALFORMED_MATCH_LINES,
    GOOD_MATCHES,
    BAD_GAME_MODE,
    BAD_LOBBY,
    LEAVERS,
    REAL_MATCH_WITH_LEAVERS,
    BAD_MATCHES
  }

  private boolean isRealMatch(KijiRowData kijiRowData) throws IOException {

    return true;
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
    int gameMode = (Integer) kijiRowData.getMostRecentCell("data", "game_mode").getData();
    if(gameMode > 6 && gameMode != 12 && gameMode != 14){
      producerContext.incrementCounter(Counters.BAD_GAME_MODE);
      realMatch = false;
    }
    Integer o = kijiRowData.getMostRecentValue("data", "lobby_type");
    LobbyType lobbyType = LobbyType.fromInt(o);
    if(!(lobbyType == LobbyType.PUBLIC_MATCHMAKING ||
        lobbyType == LobbyType.TOURNAMENT ||
        lobbyType == LobbyType.TEAM_MATCH ||
        lobbyType == LobbyType.SOLO_QUEUE)){
      producerContext.incrementCounter(Counters.BAD_LOBBY);
      realMatch = false;
    }
    Players player_data = kijiRowData.getMostRecentValue("data", "player_data");
    List<Player> players = player_data.getPlayers();
    for(Player player : players){
      LeaverStatus ls = LeaverStatus.fromInt(player.getLeaverStatus());
      if(LeaverStatus.fromInt(player.getLeaverStatus()) != LeaverStatus.STAYED){
        producerContext.incrementCounter(Counters.LEAVERS);
        if(realMatch){
          producerContext.incrementCounter(Counters.REAL_MATCH_WITH_LEAVERS);
        }
        realMatch = false;
      }
    }

    if(realMatch){
      producerContext.incrementCounter(Counters.GOOD_MATCHES);
      producerContext.put("real_match", 0L, 1.0);
    } else {
      producerContext.incrementCounter(Counters.BAD_MATCHES);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    RealMatchProducer rp = new RealMatchProducer();
    KijiProduceJobBuilder builder = KijiProduceJobBuilder.create();
    builder.withProducer(RealMatchProducer.class).withInputTable(KijiURI.newBuilder()
          .withTableName("dota_matches").withInstanceName("wibidota").build())
          .withConf(rp.getConf());
    builder.build().run();
  }
}
