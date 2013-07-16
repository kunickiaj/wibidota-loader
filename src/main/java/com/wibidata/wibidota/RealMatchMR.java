package com.wibidata.wibidota;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RealMatchMR extends Configured {

  // Not sure what to do here, how best to input the  values to Kiji?
  public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, Mapper.Context context)
       throws IOException, InterruptedException {
    }
  }
}
