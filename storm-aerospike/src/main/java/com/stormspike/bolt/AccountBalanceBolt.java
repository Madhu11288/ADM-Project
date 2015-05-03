package com.stormspike.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.stormspike.linear.road.schema.AccountBalanceTable;
import com.stormspike.linear.road.schema.VehicleInLastFiveMinsTable;
import com.stormspike.topology.Constants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;

public class AccountBalanceBolt implements IRichBolt{

    OutputCollector outputCollector;
    private AccountBalanceTable accountBalanceTable;
    File file;
    PrintWriter writer;
    private AerospikeClient client;
    private WritePolicy writePolicy;
    private VehicleInLastFiveMinsTable vehicleInLastFiveMinsTable;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.accountBalanceTable = new AccountBalanceTable();
//        this.vehicleInLastFiveMinsTable = new VehicleInLastFiveMinsTable();
        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();
        file = new File("/Users/madhushrees/ADM_/codeBase/ADM-Project/storm-aerospike/src/main/java/com/stormspike/results/ab.txt");
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
        String time = values[1];
        String queryId = values[9];
        Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_ACCOUNT_BALANCE_SET, vehicleID);
        if (this.client.exists(this.writePolicy, key)) {
            Record abRecord = this.client.get(this.writePolicy, key, Constants.ACCOUNT_BALANCE_BIN);
            String ab = abRecord.getValue(Constants.ACCOUNT_BALANCE_BIN).toString();
            writer.println("Account Balance: Time: " + time + ", " + vehicleID + ": " + ab);
        } else{
            writer.println("Account Balance: Time: " + time + ", " + vehicleID + ": " + "0");
        }
        writer.flush();
        //this.accountBalanceTable.createAccountBalanceTable(vehicleID,time,queryId);
//        this.accountBalanceTable.updateAccountBalance(vehicleID,time,queryId,0);
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