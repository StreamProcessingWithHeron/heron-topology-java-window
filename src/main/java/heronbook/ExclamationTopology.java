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

import java.util.Map;
import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;

import org.pmw.tinylog.*;

import java.util.HashMap;
import io.bretty.console.table.*;

import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.windowing.TupleWindow;
import java.util.stream.Collectors;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

    public static void main(String[] args) throws Exception {
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("word", new TestWordSpout(), 2);
      builder.setBolt("exclaim1", new ExclamationBolt()
        .withTumblingWindow(BaseWindowedBolt.Count.of(10)), 2) 
             .shuffleGrouping("word", "s1")
             .shuffleGrouping("word", "s2");
      builder.setBolt("exclaim2", new ExclamationBolt()
        .withTumblingWindow(BaseWindowedBolt.Count.of(10)), 2)
             .shuffleGrouping("exclaim1");

      Config conf = new Config();
      conf.setDebug(true);
      conf.setNumStmgrs(3);

      String topologyName = "test";
      if (args != null && args.length > 0) {
        topologyName = args[0];
      }

      HeronSubmitter.submitTopology(
        topologyName, conf, builder.createTopology()); 
    }

    public static class ExclamationBolt 
      extends BaseWindowedBolt {
        OutputCollector collector;

        @Override
        public void prepare(Map conf,
                            TopologyContext context,
                            OutputCollector collector) {
          this.collector = collector;
          TestWordSpout.setLoggging(context);
          Logger.trace("streams: {}", context.getThisStreams());
        }

        @Override
        public void execute(TupleWindow inputWindow) { 
          String output = inputWindow.get().stream() 
            .map(tuple -> {
              String srcStream = tuple.getSourceStreamId();
              if ("s2".equals(srcStream)) {
                return tuple.getStringByField("f2") + "&" +
                       tuple.getStringByField("f3");
              } else {
                return tuple.getString(0);
              }})
            .distinct().sorted().collect(Collectors.joining(","));
          Logger.trace("emit {}", output);
          collector.emit(new Values(output));
        }

        private static String tupleAsTable(Tuple tuple) {
          Map<Integer, String> tableHeader = 
            new HashMap<Integer, String>();
          for (String s: tuple.getFields()) {
            tableHeader.put(tuple.fieldIndex(s), s);
          } // table head
          Object[][] data = new Object[2][tuple.size()];
          for (int i=0; i<tuple.size(); i++) {
            data[0][i] = tableHeader.get(i);
            data[1][i] = tuple.getValue(i);
          } // table body
          return Table.of(data, Alignment.LEFT, 20).toString();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }
}
