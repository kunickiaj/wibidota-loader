package com.wibidata.wibidota;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 7/12/13
 * Time: 4:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class DotaValueHistogramMR {

  private static final Logger LOG = LoggerFactory.getLogger(DotaValueHistogram.class);

  private static final LongWritable ONE = new LongWritable(1);

  public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static JsonParser PARSER = new JsonParser();

    private static long max = Long.MIN_VALUE;

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try{
        JsonObject matchData = PARSER.parse(value.toString()).getAsJsonObject();
        int game_mode = matchData.get("game_mode" ).getAsInt();
      } catch (IllegalStateException e) {
        // Ignore
      }
    }
  }


}
