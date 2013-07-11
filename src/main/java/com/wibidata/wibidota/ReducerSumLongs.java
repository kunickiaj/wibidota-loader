package com.wibidata.wibidota;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.impl.KijiMappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReducerSumLongs extends KijiReducer<Text, LongWritable, Text, LongWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(ReducerSumLongs.class);

  public void reduce(Text key, Iterable<LongWritable> values, Reducer.Context context)
      throws IOException, InterruptedException {
    LOG.error("Called for key:" + key);
    long total = 0;

    for(LongWritable lw : values){
      total += lw.get();
    }
    context.write(key, new LongWritable(total));
  }

  public Class<? extends Writable> test(){
    return null;
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
