package com.wibidata.wibidota;

import com.wibidata.wibidota.avro.Player;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.mapreduce.tools.KijiProduce;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 7/16/13
 * Time: 6:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class WinLossProducer extends KijiProducer {
  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(Integer.MAX_VALUE)
        .add("data", "radiant_win")
        .add("data", "player")
        .add("data", "game_mode")
        .add("data", "lobby_type");
    return builder.addColumns(def).build();
  }

  @Override
  public String getOutputColumn() {
    return "derived_data";
  }

  @Override
  public void produce(KijiRowData kijiRowData, ProducerContext producerContext) throws IOException {
    int wins = 0;
    int losses = 0;
    for(Long time : kijiRowData.getTimestamps("data", "radiant_win")){
      int gameMode = (Integer) kijiRowData.getMostRecentCell("data", "game_mode").getData();
      if(gameMode > 6 && gameMode != 12 && gameMode != 14){
        break;
      }
      Integer o = kijiRowData.getMostRecentValue("data", "lobby_type");
      DotaValues.LobbyType lobbyType = DotaValues.LobbyType.fromInt(o);
      if(!(lobbyType == DotaValues.LobbyType.PUBLIC_MATCHMAKING ||
          lobbyType == DotaValues.LobbyType.TOURNAMENT ||
          lobbyType == DotaValues.LobbyType.TEAM_MATCH ||
          lobbyType == DotaValues.LobbyType.SOLO_QUEUE)){
        break;
      }
      Player self = kijiRowData.getValue("data", "player", time);
      boolean onRadiant = DotaValues.radiantPlayer(self.getPlayerSlot());
      if(onRadiant && ((Boolean) kijiRowData.getValue("data", "radiant_win", time))){
        wins++;
      } else {
        losses++;
      }
    }
    producerContext.put("wins", 0, 0.0 + wins);
    producerContext.put("losses", 0, 0.0 + losses);
  }
}
