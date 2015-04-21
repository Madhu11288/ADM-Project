package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CaptureQueryZeroBolt implements IRichBolt{

    OutputCollector outputCollector;
    JedisCluster jedis;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7000));
        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7001));
        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7002));
        jedis = new JedisCluster(jedisClusterNodes);
    }

    @Override
    public void execute(Tuple input) {
        String record = (String) input.getValue(1);
        String[] values = record.split(",");
        String minute = values[1];
        String vehicleID = values[2];
        String speed = values[3];
        String xWay = values[4];
        String lane = values[5];
        String direction = values[6];
        String segment = values[7];

        String key = "avg-speed:minute:" + minute + "vehicle-id" + vehicleID;
        if (jedis.exists(key)) {
            jedis.set(key, speed);
            jedis.expire(key, 600);
        } else {
            Float avgSpeed = (Float.parseFloat(jedis.get(key)) + Float.parseFloat(speed))/2;
            jedis.set(key, avgSpeed.toString());
        }

        String keyForSegment = "report:minute:" + minute + ":xWay:" + xWay + ":lane:"
                + lane + ":direction:" + direction + ":segment:" + segment;
        jedis.lpush(keyForSegment, key);
        jedis.expire(keyForSegment, 600);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
