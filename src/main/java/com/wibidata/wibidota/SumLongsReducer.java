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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.kiji.mapreduce.KijiReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Reducer that sums LongWritables based on Text keys
 */
public class SumLongsReducer extends KijiReducer<Text, LongWritable, Text, LongWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(SumLongsReducer.class);

  public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long total = 0;
    for(LongWritable lw : values){
      total += lw.get();
    }
    context.write(key, new LongWritable(total));
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
