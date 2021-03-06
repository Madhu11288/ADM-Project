package com.adm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TwitterStreamTopology {
    public static void main(String[] args) {
        TwitterStreamTopology twitterStreamTopology = new TwitterStreamTopology();
        twitterStreamTopology.setUpAndRunTopology(args);
    }

    private void setUpAndRunTopology(String[] args) {
        // Twitter Keys
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessTokenKey = args[2];
        String accessTokenSecret = args[3];

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("Streams", new StreamSpout(consumerKey, consumerSecret,
                accessTokenKey, accessTokenSecret), 1);
        topologyBuilder.setBolt("Tweets", new TweetBolt(), 1).shuffleGrouping("Streams");

        //Aerospike Bolt
        topologyBuilder.setBolt("AerospikeHashTagBolt", new AerospikeHashTagBolt(AerospikeConstants.AS_TEST_NS, AerospikeConstants.AS_HASHTAGSET))
                .shuffleGrouping("Streams");
        topologyBuilder.setBolt("AerospikeTweetsBolt", new AerospikeTweetsBolt(AerospikeConstants.AS_TEST_NS, AerospikeConstants.AS_STORMSET))
                .shuffleGrouping("Streams");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topologyBuilder.createTopology());
    }
}
