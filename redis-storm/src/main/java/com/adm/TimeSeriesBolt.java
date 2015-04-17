package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import twitter4j.Status;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TimeSeriesBolt implements IRichBolt {
    OutputCollector outputCollector;
    JedisPool pool;
    JedisCluster jedis;
    Integer counter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("10.0.0.100", 7000));
        jedis = new JedisCluster(jedisClusterNodes);
        this.counter = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValue(0);

        Long tweetId = tweet.getId();
        Long time_mills = tweet.getCreatedAt().getTime();

        jedis.zadd("tweet-time-series", time_mills, "tweet-id:" + tweetId);
        counter++;
    }

    @Override
    public void cleanup() {
        System.out.println("User Bolt Processed: " + this.counter + " number of tweets");
        pool.destroy();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
