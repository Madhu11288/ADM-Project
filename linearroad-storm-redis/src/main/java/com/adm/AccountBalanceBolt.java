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

public class AccountBalanceBolt implements IRichBolt{

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
        String record = (String) input.getValue(0);
        String[] values = record.split(",");
        String vehicleID = values[2];
        String vehicleAccountBalanceKey = "account-balance:vehicle-id:" + vehicleID;
        if (jedis.exists(vehicleAccountBalanceKey)) {
            System.out.println(vehicleID + ": " + jedis.get(vehicleAccountBalanceKey));
        } else {
            System.out.println(vehicleID + ": " + 0);
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
