package com.stormspike.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.stormspike.linear.road.schema.AccountBalanceTable;

import java.util.Map;

public class AccountBalanceBolt implements IRichBolt{

    OutputCollector outputCollector;
    private AccountBalanceTable accountBalanceTable;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.accountBalanceTable = new AccountBalanceTable();
    }
    @Override
    public void execute(Tuple input) {
        String record = (String) input.getValue(0);
        String[] values = record.split(",");
        String vehicleID = values[2];
        String time = values[1];
        String queryId = values[9];
        this.accountBalanceTable.createAccountBalanceTable(vehicleID,time,queryId);
       // this.accountBalanceTable.updateAccountBalance(vehicleID);
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