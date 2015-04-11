package com.stormspike.topology;

import java.util.Arrays;

import com.stormspike.bolt.AerospikeBolt;
import com.stormspike.spout.TwitterSampleSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;


public class MyTopology {
    public static final String AEROSPIKE_HOST = "127.0.0.1";
    public static final int AEROSPIKE_PORT = 3000;
    public static final String AEROSPIKE_NS = "test";
    public static final String AEROSPIKE_SET = "stormset";


    public static void main(String[] args) {
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];

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
