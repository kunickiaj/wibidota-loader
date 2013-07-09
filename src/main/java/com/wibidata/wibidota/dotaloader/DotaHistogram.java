package com.wibidata.wibidota.dotaloader;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Class to find tfields can reach in raw JSON encoded Dota matches
 */

public class DotaHistogram extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(DotaHistogram.class);

  public static long max = 0;

  private static class HistogramParameters {
    public String field;
    public Long interval;

    public HistogramParameters(String field, Long interval){
      this.field = field;
      this.interval = interval;
    }

    public Text key(JsonObject data){
      JsonElement elem = data.get(field);
      if(interval == null){
        return new Text(field + "::" + elem);
      } else {
        if(elem == null){
          return new Text(field + "::NULL");
        }
        if(elem.getAsLong() != 4294967295L && elem.getAsLong() > Integer.MAX_VALUE){
          max = elem.getAsLong();
          LOG.error("NEW MAX: " + max);
        }

        long value = elem.getAsLong() / interval;
        return new Text(field + " + (" + value*(interval-1) + "-" +
            value*interval + ")::" + value);

      }
    }
  }

  public static final HistogramParameters[] PLAYER_HISTOGRAMS =
      new HistogramParameters[]{new HistogramParameters("account_id", 1000000L)};

  public static final HistogramParameters[] MATCH_HISTOGRAMS = new HistogramParameters[]{};



  public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static JsonParser PARSER = new JsonParser();

    // Cache this
    private final static LongWritable ONE = new LongWritable(1);

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try{
        JsonObject matchData = PARSER.parse(value.toString()).getAsJsonObject();
        for(HistogramParameters hp : MATCH_HISTOGRAMS){
//          context.write(hp.key(matchData), ONE);
        }
        for (JsonElement playerElem : matchData.get("players").getAsJsonArray()) {
          JsonObject playerData = playerElem.getAsJsonObject();
          for(HistogramParameters hp : PLAYER_HISTOGRAMS){
            hp.key(playerData);
          }

        }
      } catch (IllegalStateException e) {
        // Do nothing
      }
    }
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new DotaHistogram(), args);
    System.exit(res);
  }

  public final int run(final String[] args) throws Exception {
    Job job = new Job(super.getConf(), "Dota Histogram Builder");
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setMapperClass(Map.class);
//    job.setCombinerClass(DotaEnumCounter.Add.class);
//    job.setReducerClass(DotaEnumCounter.Add.class);

    job.setJarByClass(DotaHistogram.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    if (job.waitForCompletion(true)) {
      return 0;
    } else {
      return -1;
    }
  }


}
