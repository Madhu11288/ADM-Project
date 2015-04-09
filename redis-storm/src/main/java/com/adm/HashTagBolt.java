package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Map;

public class HashTagBolt implements IRichBolt{
    OutputCollector outputCollector;
    Jedis jedis;
    Integer counter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.jedis = new Jedis("localhost");
        this.counter = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValue(0);
        HashtagEntity[] hashtagEntities = tweet.getHashtagEntities();
        if (hashtagEntities.length != 0) {
            System.out.println("Hash Counter: " + counter);
            jedis.set(counter.toString() + "-Hash", hashtagEntities[0].getText());
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
