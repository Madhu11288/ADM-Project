package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HashTagBolt implements IRichBolt{
    OutputCollector outputCollector;
    //    JedisCluster jedis;
    Jedis jedis;
    Integer counter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
//        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7000));
//        jedis = new JedisCluster(jedisClusterNodes);
        this.jedis = new Jedis("10.0.0.29");
        this.counter = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValue(0);
        HashtagEntity[] hashtagEntities = tweet.getHashtagEntities();
        if (hashtagEntities.length != 0) {
            for (HashtagEntity hashtagEntity : hashtagEntities) {
                String key = "hash-tag:" + hashtagEntity.getText().toLowerCase();
                jedis.incr(key);
                jedis.zadd("trending-topics", Double.parseDouble(jedis.get(key)), key);
            }
            counter++;
        }
    }

    @Override
    public void cleanup() {
        System.out.println("Hash Bolt Processed: " + this.counter + " number of tweets");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
