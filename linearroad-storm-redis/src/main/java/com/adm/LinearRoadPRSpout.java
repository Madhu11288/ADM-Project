package com.adm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LinearRoadPRSpout implements IRichSpout {

    SpoutOutputCollector _collector;
    BufferedReader br;
    Process process;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("entry"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        String command = "/Users/sharanyabathey/courses/mcs-spring2015/advn_data_management/project/benchmarks/linear_road/datadriver-src/datafeeder " +
                "/Users/sharanyabathey/courses/mcs-spring2015/advn_data_management/project/benchmarks/linear_road/datadriver-test-data/datafile3hours.dat";
        try {
            process = Runtime.getRuntime().exec(command);
            br = new BufferedReader(new InputStreamReader(process.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        String line;
        try {
            while ((line = br.readLine()) != null) {
                if (line.startsWith("0")) {
                    System.out.println(line);
                    this._collector.emit(new Values(line));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }
}
