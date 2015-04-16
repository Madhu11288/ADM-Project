package com.stormspike.topology;

import java.util.Arrays;

import com.stormspike.bolt.AerospikeBolt;
import com.stormspike.bolt.HashTagBolt;
import com.stormspike.spout.TwitterSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TweetsTopology {

    public static final String AEROSPIKE_NS = "test";
    public static final String AEROSPIKE_HASHTAGSET = "stormset-hashtag";
    public static final String AEROSPIKE_STORMSET = "stormset";

    public static void main(String[] args) {
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];

        String[] keyWords = {"Chicago"};
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitter", new TwitterSpout(consumerKey, consumerSecret,
                accessToken, accessTokenSecret, keyWords));
        builder.setBolt("HashTagBolt", new HashTagBolt(AEROSPIKE_NS, AEROSPIKE_HASHTAGSET))
                .shuffleGrouping("twitter");
        builder.setBolt("AerospikeBolt", new AerospikeBolt(AEROSPIKE_NS, AEROSPIKE_STORMSET))
                .shuffleGrouping("twitter");

                
        Config conf = new Config();
        
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(100000);
        cluster.shutdown();
    }
}
