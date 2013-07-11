package com.wibidata.wibidota;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.avro.*;
import org.kiji.schema.layout.KijiTableLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class DotaValuesHistogram extends KijiGatherer{

  private static final Logger LOG = LoggerFactory.getLogger(ReducerSumLongs.class);


  private static final LongWritable ONE = new LongWritable(1);

  private static interface KeyGenerator {
    public String getKey(KijiRowData row) throws IOException;
    public String[] getColumnNames();
  }

  private static class ValueByTime implements KeyGenerator {

    public final String family;
    public final String column;
    public final Long interval;
    public final String nullStr;

    public ValueByTime(String family, String column, Long interval) {
      this.family = family;
      this.column = column;
      this.interval = interval;
      this.nullStr = "NULL";
    }

    public String[] getColumnNames() {
      return new String[]{
          family +":" + column//,
//          "data:start_time"
      };
    }

    public String getKey(KijiRowData row) throws IOException {
      Integer value = row.getMostRecentValue(family, column);
      LOG.error("GOT ROW: " + row.toString());
      LOG.error(family + ":" + column + ": " + value);
      StringBuilder sb = new StringBuilder();
      sb.append(family + ":" + column + "=" + (value == null ? nullStr : value));
      if(interval != null){
        Long startTime = 10L;//row.getMostRecentValue("data", "start_time");
        long slot = startTime / interval;
        sb.append(" [" + (slot - 1) * interval + "-" + slot * interval + ")");
        LOG.error("Outputting: " + sb.toString());
      }
      return sb.toString();
    }
  }

  private static KeyGenerator[] MATCH_KEYS = new KeyGenerator[]{
      new ValueByTime("data", "game_mode", 10000L)
   };

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(1).withPageSize(100);
//    HashSet<String> colsAdded = new HashSet<String>();
//    for(KeyGenerator kg : MATCH_KEYS){
//      for(String s : kg.getColumnNames()){
//        if(!colsAdded.contains(s)){
//          def.add(new KijiColumnName(s));
//          colsAdded.add(s);
//          System.out.println(s);
//        }
//      }
//    }
    def.add(new KijiColumnName("data:game_mode"));
//         .add(new KijiColumnName("data:start_time"));
    return builder.addColumns(def).build();
  }

  @Override
  public void gather(KijiRowData input, GathererContext context) throws IOException {
    for(KeyGenerator kg: MATCH_KEYS){
      context.write(new Text(kg.getKey(input)), ONE);
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

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//  EntityIdFactory.getFactory(RowKeyFormat2.newBuilder().setComponents(null).build()).
  List<RowKeyComponent> lst = new ArrayList<RowKeyComponent>();
//  lst.add(RowKeyComponent.newBuilder().setType(ComponentType.LONG).setName("?").build());
  EntityIdFactory d = EntityIdFactory.getFactory(RowKeyFormat2.newBuilder().
      setComponents(lst).setSalt(HashSpec.newBuilder().setHashType(HashType.MD5).).build());
    KijiMapReduceJob job = KijiGatherJobBuilder.create()
        .withInputTable(KijiURI.newBuilder()
            .withTableName("dota_matches").withInstanceName("wibidota").build())
        .withGatherer(DotaValuesHistogram.class)
        .withReducer(ReducerSumLongs.class)
        .withCombiner(ReducerSumLongs.class)
        .withOutput(MapReduceJobOutputs.newTextMapReduceJobOutput(new Path("hdfs://localhost:8020/counts"), 1))
         .withConf(new Configuration())
//         .withStartRow(d.getEntityId("107378376"))
//         .withLimitRow(d.getEntityId("107814591"))
        .build();
    job.run();
  }
}
