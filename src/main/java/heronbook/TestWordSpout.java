/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package heronbook;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.utils.Utils;

import org.pmw.tinylog.*;
import org.pmw.tinylog.writers.*;

public class TestWordSpout extends BaseRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;

    public TestWordSpout() {
        this(true);
    }

    public TestWordSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    public static void setLoggging(TopologyContext context) {
      LoggingContext.put("tId", context.getThisTaskId()); 
      LoggingContext.put("cId", context.getThisComponentId()); 
      LoggingContext.put("tIdx", context.getThisTaskIndex()); 
      Configurator.currentConfig()
        .formatPattern("{date:yyyy-MM-dd HH:mm:ss} "
          + "/{context:tId}({context:cId}[{context:tIdx}])/ " 
          + "[{thread}]\n{class}.{method}() {level}:\n{message}")
        .writer(new SharedFileWriter("/tmp/log.txt", true), 
                Level.TRACE)
        .activate();
    }

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
      this.collector = collector;
      setLoggging(context);
      Logger.trace("streams: {}", context.getThisStreams());
    }

    @Override
    public void close() {
      Logger.trace("close");
    }

    @Override
    public void nextTuple() {
      Utils.sleep(1000); 
      final String[] words = new String[] {
        "nathan", "mike", "jackson", "golda", "bertels"};
      final Random rand = new Random();
      final String w1 = words[rand.nextInt(words.length)]; 
      final String w2 = words[rand.nextInt(words.length)];
      final String w3 = words[rand.nextInt(words.length)];
      final Values v1 = new Values(w1); 
      final Values v2 = new Values(w2, w3);
      final Integer msgId = Integer.valueOf(rand.nextInt()); 
      collector.emit("s1", v1, msgId); 
      collector.emit("s2", v2);
      Logger.trace("emit {} {} {}", "s1", v1, msgId); 
      Logger.trace("emit {} {}", "s2", v2);
    }

    @Override
    public void ack(Object msgId) {
      Logger.trace("ack {}", msgId);
    }

    @Override
    public void fail(Object msgId) {
      Logger.trace("fail {}", msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declareStream("s1", new Fields("f1"));
      declarer.declareStream("s2", new Fields("f2", "f3"));
    }
}
