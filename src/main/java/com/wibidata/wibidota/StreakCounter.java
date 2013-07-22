package com.wibidata.wibidota;

import com.wibidata.wibidota.avro.Player;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;
import java.util.Arrays;

public class StreakCounter extends KijiGatherer {

  private static final int BURN_IN =  40;

  private static final long MIN = 60;

  private static final long HOUR = 60 * MIN;

  private static final long DAY = HOUR * 24;

  long[] INTERVALS = new long[]{MIN *15, MIN * 30, HOUR, HOUR * 2,
      HOUR * 6, HOUR * 12, DAY, DAY * 2, DAY * 3, DAY * 7, DAY * 14,
      Integer.MAX_VALUE};

  private static final LongWritable ONE = new LongWritable(1l);

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(Integer.MAX_VALUE)
        .add("data", "radiant_win")
        .add("data", "game_mode")
        .add("data", "duration")
        .add("data", "lobby_type")
        .add("data", "player");
    return builder.addColumns(def).build();
  }

  @Override
  public void gather(KijiRowData kijiRowData, GathererContext gathererContext) throws IOException {
    int game = 0;
    int score = 0;
    boolean[] streaking = new boolean[INTERVALS.length];
    long prevTime = 0;

    for(Long time : kijiRowData.getTimestamps("data", "game_mode").descendingSet()){

      // Make sure this is a 'serious' game
      Integer gameMode = kijiRowData.getValue("data", "game_mode", time);
      if(gameMode > 6 && gameMode != 12 && gameMode != 14){
        continue;
      }
      DotaValues.LobbyType lobbyType = DotaValues.
          LobbyType.fromInt((Integer)
          kijiRowData.getValue("data", "lobby_type", time));
      if(!(lobbyType == DotaValues.LobbyType.PUBLIC_MATCHMAKING ||
          lobbyType == DotaValues.LobbyType.TOURNAMENT ||
          lobbyType == DotaValues.LobbyType.TEAM_MATCH ||
          lobbyType == DotaValues.LobbyType.SOLO_QUEUE)){
        continue;
      }

      // Check if we won
      Player self = kijiRowData.getValue("data", "player", time);
      boolean radiantWin = (Boolean) kijiRowData.getValue("data", "radiant_win", time);
      boolean radiantPlayer = DotaValues.radiantPlayer(self.getPlayerSlot());
      boolean winner = (radiantWin && radiantPlayer) || (!radiantWin && !radiantPlayer);

      // Check what streaks still apply
      game++;
      long diff = time - prevTime -
          ((Number) kijiRowData.getValue("data", "duration", time)).longValue();
      for(int i = 0; i < INTERVALS.length; i++){
        if(diff > INTERVALS[i]){
          streaking[i] = false;
        }
      }

      // Write updates for the streaks we are tracking
      if(game > BURN_IN){
        for(int i = 0; i < INTERVALS.length; i++){
          if(streaking[i]){
            String key = "interval=" + INTERVALS[i] + ",score=" + score + ",win=" + winner;
            gathererContext.write(new Text(key), ONE);
          }
        }
      }

      // Update out win counter for the next iteration
      if(winner){
        if(score > 0){
          score++;
        } else {
          score = 1;
          Arrays.fill(streaking, true);
        }
      } else {
        if(score < 0){
          score--;
        } else {
          score = -1;
          Arrays.fill(streaking, true);
        }
      }
      prevTime = time;
    }
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return LongWritable.class;
  }
}
