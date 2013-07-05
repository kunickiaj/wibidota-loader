/* Copyright 2013 WibiData, Inc.
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

package com.wibidata.wibidota.dotaloader;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * A simple class that represents a winrate job.
 */
public class DotaScan extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(DotaScan.class);

  /**
   * Some useful counters to keep track of while processing match history.
   */
  static enum Counters {
    MALFORMED_MATCH_LINES,
    NON_MATCHMAKING_MATCHES,
    GAME_MODE_ZERO,
    MAX_INT_VALUE
  }

  /*
   * A mapper that reads over matches from the input files and outputs <hero id, match result>
   * key value pairs where a 1 is a victory and a 0 is a loss.
   */
  public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private final static Set<Integer>  leaverStatus = new HashSet<Integer>();
    private final static JsonParser PARSER = new JsonParser();
    private final static IntWritable LEAVER_KEY = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try{
        JsonObject matchTree = PARSER.parse(value.toString()).getAsJsonObject();
        // Go through the player list and output the appropriate result information.
        for (JsonElement playerElem : matchTree.get("players").getAsJsonArray()) {
          JsonElement leaver_status = playerElem.getAsJsonObject().get("leaver_status");
          int leaver_status_int;
          if(leaver_status == null){
            leaver_status_int = Integer.MIN_VALUE/2;
          } else {
              leaver_status_int = leaver_status.getAsInt();
          }
          if(!leaverStatus.contains(leaver_status_int)){
            LOG.error("FOUND LEAVER STATUS " + leaver_status_int);
            context.write(LEAVER_KEY, new IntWritable(leaver_status_int));
          }
          leaverStatus.add(leaver_status_int);
        }
      } catch (IllegalStateException e) {
        // Indicates malformed JSON.
        context.getCounter(Counters.MALFORMED_MATCH_LINES).increment(1);
      }
    }
  }

  /**
   * A simple reducer that simply averages the results to find a winrate.
   * TODO: Output hero names instead.
   */
  public static class Reduce
      extends Reducer<IntWritable, IntWritable, IntWritable, String> {


    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      String out =  key.toString() + " : ";
      for (IntWritable result : values) {
        out += values.toString() + ", ";
      }
      context.write(key, out);
    }
  }

  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new DotaScan(), args);
    System.exit(res);
  }

  public final int run(final String[] args) throws Exception {
    Job job = new Job(super.getConf(), "winrate");
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(DotaScan.class);
//    DistributedCache.addFileToClassPath(new Path("/home/chris/kiji/wibidota-loader/lib/gson-2.2.2.jar"),
//            job.getConfiguration());

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

