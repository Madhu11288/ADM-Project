package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.*;

public class QueryZeroBoltPositionReport implements IRichBolt{

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
        String position = values[8];

        String key = "position-report:vehicle-id:" + vehicleID;
        HashMap<String, String> positionReport = new HashMap<String, String>();

        positionReport.put("minute", minute);
        positionReport.put("speed", speed);
        positionReport.put("xWay", xWay);
        positionReport.put("lane", lane);
        positionReport.put("direction", direction);
        positionReport.put("segment", segment);
        positionReport.put("position", position);

        if (!jedis.exists(key)) {
            jedis.hmset(key, positionReport);
            jedis.incrBy("account-balance:vehicle-id:" + vehicleID, 0);
        } else {
            List<String> storedValues = jedis.hmget(key, "xWay", "lane", "direction", "segment", "minute");
            if (!storedValues.get(0).equals(xWay) || !storedValues.get(1).equals(lane)
                    || !storedValues.get(2).equals(direction) || !storedValues.get(3).equals(segment)) {
                // Need to do toll calculation here and remove this vehicle from previous lane entry
            } else {
                jedis.hmset(key, positionReport);
            }
        }
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
