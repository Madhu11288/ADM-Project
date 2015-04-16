package com.adm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

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
        topologyBuilder.setBolt("Tweets", new TweetBolt(), 1).allGrouping("Streams");
//        topologyBuilder.setBolt("Users", new UserBolt(), 1).allGrouping("Streams");
        topologyBuilder.setBolt("HashTags", new HashTagBolt(), 1).allGrouping("Streams");
        topologyBuilder.setBolt("TweetTimeSeries", new TimeSeriesBolt(), 1).allGrouping("Streams");
//        topologyBuilder.setBolt("Retweets", new ReTweetBolt(), 1).allGrouping("Streams");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topologyBuilder.createTopology());
        Utils.sleep(180000);
        cluster.shutdown();
    }
}
