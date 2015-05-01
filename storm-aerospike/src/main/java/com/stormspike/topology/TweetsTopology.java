package com.stormspike.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.stormspike.bolt.*;
import com.stormspike.spout.LinearRoadSpout;
import com.stormspike.spout.TwitterSpout;

public class TweetsTopology {

    public static final String AEROSPIKE_NS = "test";
    public static final String AEROSPIKE_HASHTAGSET = "stormset-hashtag";
    public static final String AEROSPIKE_STORMSET = "stormset";

    public static void main(String[] args) {
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter", new TwitterSpout(consumerKey, consumerSecret,
                accessToken, accessTokenSecret));

        builder.setBolt("HashTagBolt", new AerospikeHashTagBolt(AEROSPIKE_NS, AEROSPIKE_HASHTAGSET))
                .shuffleGrouping("twitter");
        builder.setBolt("AerospikeBolt", new AerospikeTweetsBolt(AEROSPIKE_NS, AEROSPIKE_STORMSET))
                .shuffleGrouping("twitter");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());

        Utils.sleep(100000);
        cluster.shutdown();
    }
}
