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

import java.io.IOException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A Map Reduce job built to gather example matches for each value
 * and enum could take, Currently only works for fields in Player objects
 */
public class DotaCheckValues extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(DotaTest.class);

    static enum Counters {
        MALFORMED_MATCH_LINES
    }

    public static int playerSlot(int bits){
        int slot = 0;
        if(bits >> 7 == 1){
            slot += 5;
        }
        return slot + (bits & 7)  + 1;
    }

    // Maximum number of examples to print per a value

    /**
     * A Mapper class that sends a limited number of examples
     * to the reducers
     */
    public static class EnumGatherMap extends Mapper<LongWritable, Text, Text, Text> {

      private final static JsonParser PARSER = new JsonParser();

      // Just to caches this value
      private static final IntWritable ONE = new IntWritable(1);

      private static class Range {
        long min;
        long max;
        boolean allowNull;

        public Range(){
          min = 0;
          max = Integer.MAX_VALUE;
        }

        public Range(int max, int min){
          this.min = min;
          this.max = max;
          allowNull = false;
        }

        public Range(long max, long min, boolean allowNull){
          this.min = min;
          this.max = max;
          this.allowNull = allowNull;
        }

        public String check(JsonElement other){
          if(other == null && !allowNull){
            return "NULL";
          }
          long i = other.getAsLong();
          if(i < min || i > max){
            return other + "NOT BETWEEN " + min + " AND " + max;
          }
          return null;
        }
      }

      private static final HashMap<String, Range> RANGES;
        static {
          RANGES = new HashMap<String, Range>();
          RANGES.put("dire_tower_status", new Range(0, (int) Math.pow(2,22)));
          RANGES.put("radiant_tower_status", new Range(0, (int) Math.pow(2,22)));
          RANGES.put("dire_barracks_status", new Range(0, (int) Math.pow(2,6)));
          RANGES.put("radiant_barracks_status", new Range(0, (int) Math.pow(2,6)));
          RANGES.put("cluster", new Range());
          RANGES.put("first_blood_time", new Range());
          RANGES.put("duration", new Range());
          RANGES.put("season", new Range(0, Integer.MAX_VALUE, true));
          RANGES.put("start_time", new Range(0, Long.MAX_VALUE, true));
          RANGES.put("match_id", new Range(0, Long.MAX_VALUE, true));

        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try{
                JsonObject matchData = PARSER.parse(value.toString()).getAsJsonObject();

            } catch (IllegalStateException e) {
                // Indicates malformed JSON.
                context.getCounter(Counters.MALFORMED_MATCH_LINES).increment(1);
            }
        }
    }

    /**
     * Combiner class that aggregates lines
     */
    public static class EnumGatherCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder out = new StringBuilder();
            for(Text text : values){
                out.append(text + "\n");
            }
            context.write(key, new Text(out.toString()));
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
        int res = ToolRunner.run(conf, new DotaCheckValues(), args);
        System.exit(res);
    }

    public final int run(final String[] args) throws Exception {
        Job job = new Job(super.getConf(), "Dota Enum Gatherer");

        if (job.waitForCompletion(true)) {
            return 0;
        } else {
            return -1;
        }
    }
}
