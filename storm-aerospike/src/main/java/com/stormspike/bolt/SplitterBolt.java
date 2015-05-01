package com.stormspike.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class SplitterBolt implements IRichBolt {

    OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String record = (String) tuple.getValue(0);
        if(record.startsWith("0"))
        {
            outputCollector.emit("positionReportStream", new Values(record));
        }
        else if(record.startsWith("2"))
        {
            outputCollector.emit("accountBalanceStream", new Values(record));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("positionReportStream", new Fields("positionReport"));
        outputFieldsDeclarer.declareStream("accountBalanceStream", new Fields("accountBalance"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
