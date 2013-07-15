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

package com.wibidata.wibidota;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.kiji.mapreduce.lib.reduce.LongSumReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * A Map Reduce job built to gather statistics about the different values various fields stored
 * in raw Dota match JSON data can take. The output can be subdivided by intervals
 * of matches (based on match sequence numbers) so we can, for instances, see if new records take
 * on values older ones did not. The output will be pairs of the form
 * column::value (<start_match_sequence_num>-<end_match_sequence_num> <number of occurances>
 *
 * Used to do some pre-analysis on the values to help guide table construction.
 */
public class DotaValuesCounter extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(DotaValuesCounter.class);

  static enum Counters {
    MALFORMED_MATCH_LINES
  }

  // Change these fields to change behavior

  // The fields in the JSON to track
  private static final String[] MATCH_FIELDS = new String[]{"game_mode"};

  // The fields in the player JSONObjects to track
  private static final String[] PLAYER_FIELDS = new String[]{"leaver_status"};

  // Interval to subdived the results by, can be null
  private static final Integer INTERVAL = 1000000;

  // Convert a potentially null object to a string
  private static String safeToString(Object o){
      return ( o == null ? "null" : o.toString());
  }

  /**
   * A Mapper class that simply outputs keys composed of the field, value, and interval
   * with value 1 for every record it reads
   */
  public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static JsonParser PARSER = new JsonParser();

    // Just to caches this value
    private static final LongWritable ONE = new LongWritable(1);

    private static String toReturnKey(String field, long slot, String value){
        return field + "::" + value +
               (INTERVAL == null ? "" :
               " (" + slot * INTERVAL + "-" + (slot + 1) * INTERVAL + ")");
    }

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try{
        JsonObject matchData = PARSER.parse(value.toString()).getAsJsonObject();
        long seqNum = matchData.get("match_seq_num").getAsLong();
        long slot = seqNum / INTERVAL;
        for(String field : MATCH_FIELDS){
          context.write(new Text(toReturnKey(field, slot, safeToString(matchData.get(field)))), ONE);
        }

        for (JsonElement playerElem : matchData.get("players").getAsJsonArray()) {
          JsonObject playerData = playerElem.getAsJsonObject();
          for(String field : PLAYER_FIELDS){
            context.write(new Text(toReturnKey(field, slot, safeToString(playerData.get(field)))), ONE);
          }
        }
      } catch (IllegalStateException e) {
        // Indicates malformed JSON.
        context.getCounter(Counters.MALFORMED_MATCH_LINES).increment(1);
      }
    }
  }

  /**
   * Runs the job, requires that the gson package is made
   * available to the cluster.
   *
   * @args Should contain the input and output file path
   * @throws Exception is there was a problem running the job
   */
  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new DotaValuesCounter(), args);
    System.exit(res);
  }

  public final int run(final String[] args) throws Exception {
    Job job = new Job(super.getConf(), "Dota Value Counter");
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setMapperClass(Map.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);

    job.setJarByClass(DotaValuesCounter.class);

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

