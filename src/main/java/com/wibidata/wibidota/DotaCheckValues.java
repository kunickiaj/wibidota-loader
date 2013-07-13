package com.wibidata.wibidota;

import com.wibidata.wibidota.avro.AbilityUpgrade;
import com.wibidata.wibidota.avro.AdditionalUnit;
import com.wibidata.wibidota.avro.Player;
import com.wibidata.wibidota.avro.Players;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DotaCheckValues extends KijiGatherer {

  private static final Logger LOG = LoggerFactory.getLogger(KijiGatherer.class);

  private static int rows = 0;

  private static final String[] TEAMS = new String[]{"dire", "radiant"};

  private static final String[] INT_MATCHES = new String[]{
      "cluster", "season", "league_id", "first_blood_time"
  };

  public static class BadFormat extends RuntimeException {
    public BadFormat(String msg){
      super(msg);
    }
  }

  public static void checkIntNullable(Integer i, String field){
    if(i == null){
      return;
    }
    if(i < 0 || i >= Integer.MAX_VALUE){
      throw new BadFormat(field);
    }
  }

  public static void checkInt(Integer i, String field){
    if(i == null || i < 0 || i >= Integer.MAX_VALUE/10){
      throw new BadFormat(field);
    }
  }

  public static void checkInt(Integer i, String field, int min, int max){
    if(i == null || i < min || i > max){
      throw new BadFormat(field);
    }
  }

  public static void checkDouble(Double i, String field, double min, double max){
    if(i == null || i < min || i >= max){
      throw new BadFormat(field);
    }
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(1);
    // Everything
    def.add(new KijiColumnName("data","player_data"))
        .add(new KijiColumnName("data", "match_id"))
        .add(new KijiColumnName("data", "dire_towers_status"))
        .add(new KijiColumnName("data", "radiant_towers_status"))
        .add(new KijiColumnName("data", "dire_barracks_status"))
        .add(new KijiColumnName("data", "radiant_barracks_status"))
        .add(new KijiColumnName("data", "cluster"))
        .add(new KijiColumnName("data", "season"))
        .add(new KijiColumnName("data", "start_time"))
        .add(new KijiColumnName("data", "game_mode"))
        .add(new KijiColumnName("data", "match_seq_num"))
        .add(new KijiColumnName("data", "league_id"))
        .add(new KijiColumnName("data", "first_blood_time"))
        .add(new KijiColumnName("data", "negative_votes"))
        .add(new KijiColumnName("data", "duration"))
        .add(new KijiColumnName("data", "radiant_win"))
        .add(new KijiColumnName("data", "positive_votes"))
        .add(new KijiColumnName("data", "lobby_type"))
        .add(new KijiColumnName("data", "human_players"));
    return builder.addColumns(def).build();
  }

  @Override
  public void gather(KijiRowData kijiRowData, GathererContext gathererContext) throws IOException {
    rows++;
    if(rows % 1000 == 0){
      LOG.error("Processed row: " + rows);
    }
    try {
      for(String team : TEAMS){
        Integer towerStatus = kijiRowData.getMostRecentValue("data", team + "_towers_status");
        if(towerStatus < 0 || towerStatus > Math.pow(2, 11)){
          throw new BadFormat("tower status");
        }
      }
      for(String team : TEAMS){
        Integer raxStatus = kijiRowData.getMostRecentValue("data", team + "_barracks_status");
        if(raxStatus < 0 || raxStatus > Math.pow(2, 6)){
          throw new BadFormat("rax status");
        }
      }
      checkInt((Integer) kijiRowData.getMostRecentValue("data", "human_players"), "human_players", 0, 10);

      DotaValues.LobbyType.fromInt((Integer) kijiRowData.getMostRecentValue("data", "lobby_type"));
      DotaValues.GameMode.fromInt((Integer) kijiRowData.getMostRecentValue("data", "game_mode"));
      for(String s : new String[]{"cluster", "season", "league_id", "duration",
          "negative_votes", "positive_votes"}){
        Integer n = kijiRowData.getMostRecentValue("data", s);
        if(n < 0 || n > Integer.MAX_VALUE / 2){
          throw new BadFormat(s);
        }
      }
      Players players = kijiRowData.getMostRecentValue("data", "player_data");
      for(Player player : players.getPlayers()){
        DotaValues.LeaverStatus.fromInt(player.getLeaverStatus());
        checkInt(player.getAssists(), "assists");
        checkInt(player.getDeaths(), "deaths");
        checkInt(player.getDenies(), "denies");
        checkInt(player.getGold(), "gold");
        checkInt(player.getGoldSpent(), "gold_spend");
        checkInt(player.getHeroDamage(), "hero_damage");
        checkInt(player.getHeroHealing(), "hero_healing");
        checkInt(player.getHeroId(), "hero_id", 0, 200);
        checkInt(player.getKills(), "kills");
        checkInt(player.getLastHits(), "last_hits");
        checkInt(player.getPlayerSlot(), "player_slot", 0, 256);
        checkInt(player.getLevel(), "level", 0, 25);
        checkDouble(player.getGoldPerMinute(), "gold_per_minute", 0, 50000);
        checkDouble(player.getExpPerMinute(), "exp_per_minute", 0, 50000);
        List<AbilityUpgrade> aus = player.getAbilityUpgrades();
        checkInt(aus.size(), "ability upgrade size", 0, 25);
        for(AbilityUpgrade au : aus){
          checkInt(au.getLevel(), "ability upgrade level", 0, 25);
          checkInt(au.getTime(), "ability upgrade time");
          checkInt(au.getAbilityId(), "ability id");
        }
      }
    } catch (RuntimeException re){
      System.out.println(kijiRowData.getMostRecentValue("data","match_id"));
      LOG.info(kijiRowData.getMostRecentValue("data","match_id") + "");
      throw(re);
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
