package com.stormspike.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.stormspike.topology.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class LinearRoadSpout implements IRichSpout {

    SpoutOutputCollector _collector;
    AerospikeClient aerospikeClient;
    BufferedReader br;
    Process process;
    WritePolicy aerospikeWritePolicy;

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

        this.aerospikeClient = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        this.aerospikeWritePolicy = new WritePolicy();
        this.aerospikeWritePolicy.maxRetries = 10;
        this.aerospikeWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;

        String command = "/Users/madhushrees/ADM_/datadriver/datafeeder " +
                "/Users/madhushrees/ADM_/datadriver/cardatapoints.out";

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
                this._collector.emit(new Values(line));
                Thread.sleep(5);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
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
