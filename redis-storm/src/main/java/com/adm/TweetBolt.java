package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TweetBolt implements IRichBolt{
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
        Map<String, String> hMap = new HashMap<String, String>();

        hMap.put("user-name", tweet.getUser().getName());
        if (tweet.getGeoLocation() != null)
            hMap.put("geo-location", tweet.getGeoLocation().toString());

        hMap.put("tweet-text", tweet.getText());

        hMap.put("readable-time", tweet.getCreatedAt().toString());

        Long time_mills = tweet.getCreatedAt().getTime();
        hMap.put("time-milliseconds", time_mills.toString());

        StringBuilder hashTagsBuilder = new StringBuilder();
        HashtagEntity[] hashTagEntities = tweet.getHashtagEntities();
        if (hashTagEntities.length > 0) {
            for (HashtagEntity hashtagEntity : hashTagEntities) {
                hashTagsBuilder.append(hashtagEntity.getText()).append(":");
            }
            String hashTag = hashTagsBuilder.substring(0, hashTagsBuilder.length() - 2);
            hMap.put("hash-tags", hashTag);
        }

        jedis.hmset("TWEET:" + tweetId.toString(), hMap);
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
