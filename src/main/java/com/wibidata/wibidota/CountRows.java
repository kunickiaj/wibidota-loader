package com.wibidata.wibidota;

import org.apache.hadoop.io.LongWritable;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 7/15/13
 * Time: 4:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class CountRows extends KijiGatherer<LongWritable, LongWritable> {

  private static final LongWritable ONE = new LongWritable(1L);

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(1);
    def.add("data", "match_id");
    return builder.addColumns(def).build();
  }


  @Override
  public void gather(KijiRowData kijiRowData, GathererContext<LongWritable, LongWritable> context) throws IOException {
    context.write(ONE, ONE);
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return LongWritable.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return LongWritable.class;
  }

}
