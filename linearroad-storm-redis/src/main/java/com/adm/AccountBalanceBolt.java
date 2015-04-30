package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AccountBalanceBolt implements IRichBolt{

    OutputCollector outputCollector;
//    JedisCluster jedis;
    Jedis jedis;
    File file;
    PrintWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.jedis = new Jedis("localhost");
//        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7000));
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7001));
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7002));
//        jedis = new JedisCluster(jedisClusterNodes);
        file = new File("/tmp/accountBalance");
        try {
            writer = new PrintWriter(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
            String record = (String) input.getValue(0);
            String[] values = record.split(",");
            String vehicleID = values[2];
            String vehicleAccountBalanceKey = "account-balance:vehicle-id:" + vehicleID;
            if (jedis.exists(vehicleAccountBalanceKey)) {
                writer.println("Account Balance: Time: " + values[1] + ", " +vehicleID + ": " + jedis.get(vehicleAccountBalanceKey));
            } else {
                writer.println("Account Balance: Time: " + values[1] + ", " +vehicleID + ":0");
            }
        writer.flush();
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
