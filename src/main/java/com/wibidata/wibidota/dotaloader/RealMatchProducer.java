package com.wibidata.wibidota.dotaloader;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import com.wibidata.wibidota.dotaloader.DotaValues.Columns;
import com.wibidata.wibidota.dotaloader.DotaValues.LeaverStatus;
import com.wibidata.wibidota.dotaloader.DotaValues.LobbyType;

import java.io.IOException;
import java.util.List;

public class RealMatchProducer extends KijiProducer {

  private static boolean isRealMatch(KijiRowData kijiRowData) throws IOException {
    int gameMode = (Integer) kijiRowData.getMostRecentCell("data", "game_mode").getData();
    if(gameMode > 6 && gameMode != 12 && gameMode != 14){
      return false;
    }
    LobbyType lobbyType =
        LobbyType.fromInt((Integer) kijiRowData.getMostRecentCell("data", "lobby_type").
            getData());
    if(!(lobbyType == LobbyType.PUBLIC_MATCHMAKING ||
        lobbyType == LobbyType.TOURNAMENT ||
        lobbyType == LobbyType.TEAM_MATCH ||
        lobbyType == LobbyType.SOLO_QUEUE)){
      return false;
    }
    List<Player> players = kijiRowData.getMostRecentValue("data", "player_data");
    for(Player player : players){
      LeaverStatus ls = LeaverStatus.fromInt(player.getLeaverStatus());
      if(LeaverStatus.fromInt(player.getLeaverStatus()) == LeaverStatus.STAYED){
        return false;
      }
    }
    return true;
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    return builder.addColumns(builder.newColumnsDef()
        .withMaxVersions(1).withPageSize(100)
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
    if(isRealMatch(kijiRowData)){
      producerContext.put("real_match", 0.0);
    }
  }
}
