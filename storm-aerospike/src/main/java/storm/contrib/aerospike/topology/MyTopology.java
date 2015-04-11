/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.contrib.aerospike.topology;

import java.util.Arrays;

import storm.contrib.aerospike.bolt.AerospikeBolt;
import storm.contrib.aerospike.bolt.AerospikePersistBolt;
import storm.contrib.aerospike.bolt.PrinterBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.contrib.aerospike.spout.TwitterSampleSpout;


public class MyTopology {
    public static final String AEROSPIKE_HOST = "127.0.0.1";
    public static final int AEROSPIKE_PORT = 3000;
    public static final String AEROSPIKE_NS = "test";
    public static final String AEROSPIKE_SET = "stormset";


    public static void main(String[] args) {
        String consumerKey = "2mENRlpi5aGK7FRzKnl0c2h1X";
        String consumerSecret = "Kq1J74XDInqMl4MFQwGutxDR92MtNKq9Qc7aKfyZN8JCNcnTuI";
        String accessToken = "850496430-YMenq9JAlax4fgeodkVTtVAafa49GXT0evpe3aQZ";
        String accessTokenSecret = "K0eXyBqGEoh2pJ1sRqB2TcCRSYcaJBUYTVKRq0rhcWofP";
        String[] arguments = {consumerKey,consumerSecret,accessToken,accessTokenSecret};
        String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, keyWords));
        builder.setBolt("print", new AerospikeBolt(AEROSPIKE_HOST, AEROSPIKE_PORT, AEROSPIKE_NS, AEROSPIKE_SET))
                .shuffleGrouping("twitter");

                
        Config conf = new Config();
        
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(50000);
        cluster.shutdown();
    }
}
