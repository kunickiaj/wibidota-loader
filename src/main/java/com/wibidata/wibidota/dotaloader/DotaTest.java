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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.Set;
import java.util.HashSet;

/**
 * A Map Reduce job built to gather statistics about the different values various fields stored
 * in raw Dota match JSON data can take. The output can be subdivided by intervals
 * of matches so we can, for instances, see if new records take on values older ones did not.
 */
public class DotaTest extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(DotaTest.class);

  static enum Counters {
    MALFORMED_MATCH_LINES
  }

  /**
   * A Mapper class that simply outputs keys composed of the field, value, and interval
   * with value 1 for every record it reads
   */
  public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static JsonParser PARSER = new JsonParser();

    // Just to caches this value
    private static final LongWritable ONE = new LongWritable(1);

    private static final Set<Integer> LEAVER_STATUS = new HashSet<Integer>();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try{
        JsonObject matchData = PARSER.parse(value.toString()).getAsJsonObject();
        for (JsonElement playerElem : matchData.get("players").getAsJsonArray()) {
          JsonObject playerData = playerElem.getAsJsonObject();
          JsonElement leaverStatus = playerData.get("leaver_status");
          int leaverStatusInt = (leaverStatus == null ? -10 : leaverStatus.getAsInt());
          if(!LEAVER_STATUS.contains(leaverStatusInt)){
              LOG.error("Found:" + leaverStatusInt + "\n" + value.toString());
          }
          LEAVER_STATUS.add(leaverStatusInt);
        }
      } catch (IllegalStateException e) {
        // Indicates malformed JSON.
        context.getCounter(Counters.MALFORMED_MATCH_LINES).increment(1);
      }
    }
  }

  /**
    * Reducer class that aggregates counts, should also be used as a
    * combiner
    */
  public static class Reduce
      extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
        long total = 0;
        for(LongWritable lw : values){
            total += lw.get();
        }
        context.write(key, new LongWritable(total));
    }
  }

  /**
   * Runs the job, requires that the gson package is made
   * available to the cluster.
   *
   * @args Should contain the input and output file
   * @throws Exception is there was a problem running the job
   */
  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new DotaTest(), args);
    System.exit(res);
  }

  public final int run(final String[] args) throws Exception {
    Job job = new Job(super.getConf(), "Dota Enum Counter");
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setMapperClass(Map.class);

    job.setJarByClass(DotaTest.class);

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

