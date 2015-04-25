package com.stormspike.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class ForwarderBolt implements IRichBolt {
    OutputCollector outputCollector;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String record = (String) input.getValue(0);
        if (record.startsWith("0")) {
            outputCollector.emit(new Values(0, record));
        } else if (record.startsWith("2") || record.startsWith("3") || record.startsWith("4")) {
            System.out.println("OTHER: " + record);
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("query-type", "report"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
