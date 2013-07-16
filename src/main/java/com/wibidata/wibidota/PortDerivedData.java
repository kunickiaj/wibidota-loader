package com.wibidata.wibidota;

import com.wibidata.wibidota.avro.Players;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;

public class PortDerivedData extends KijiGatherer<LongWritable, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(DotaGatherExampleValues.class);

  private static final KijiURI kijiURI = KijiURI.newBuilder().withInstanceName("wibidota").build();
  private static KijiTable table;
  private static KijiTableWriter writer;
  private static Kiji kiji;

  @Override
  public void setup(GathererContext context){
    try {
      kiji = Kiji.Factory.open(kijiURI);
      table = kiji.openTable("dota_players");
      writer = table.openTableWriter();
    } catch (IOException e) {
      throw new RuntimeException("Unable to open table: " + kijiURI.toString());
    }
  }

  @Override
  public void cleanup(GathererContext context){
    try {
      writer.close();
      table.release();
      kiji.release();
    } catch (IOException e) {
      throw new RuntimeException("Unable to release table: " + kijiURI.toString());
    }
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(1);
    def.addFamily("derived_data");
    def.add("data", "player_data");
    return builder.addColumns(def).build();
  }

  @Override
  public void gather(KijiRowData kijiRowData, GathererContext<LongWritable, Text> longWritableTextGathererContext) throws IOException {
    Players players = kijiRowData.getMostRecentValue("data", "players'");
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return LongWritable.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return Text.class;
  }
}
