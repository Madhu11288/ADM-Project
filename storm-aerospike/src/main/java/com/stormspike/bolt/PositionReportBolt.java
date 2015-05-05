package com.stormspike.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.stormspike.linear.road.schema.AccountBalanceTable;
import com.stormspike.linear.road.schema.AverageSpeedTable;
import com.stormspike.linear.road.schema.PositionReportTable;
import com.stormspike.linear.road.schema.VehicleInLastFiveMinsTable;
import com.stormspike.topology.Constants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;

public class PositionReportBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    PositionReportTable positionReportTable;
    AverageSpeedTable averageSpeedTable;
    VehicleInLastFiveMinsTable vehicleInLastFiveMinsTable;
    AccountBalanceTable accountBalanceTable;

    File positionReportFile;
    PrintWriter positionReportWriter;
    private File accountBalanceFile;
    private PrintWriter accountBalanceWriter;
    private AerospikeClient client;
    private WritePolicy writePolicy;
    private File accountTimeFile;
    private PrintWriter accountTime;
    private File positionTimeFile;
    private PrintWriter positionTime;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.positionReportTable = new PositionReportTable();
        this.averageSpeedTable = new AverageSpeedTable();
        this.vehicleInLastFiveMinsTable = new VehicleInLastFiveMinsTable();
        this.accountBalanceTable = new AccountBalanceTable();

        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();

        positionReportFile = new File("/tmp/positionReport");
        try {
            positionReportWriter = new PrintWriter(positionReportFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        accountBalanceFile = new File("/tmp/accountBalance");
        try {
            accountBalanceWriter = new PrintWriter(accountBalanceFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        accountTimeFile = new File("/tmp/accountTime");
        try {
            accountTime = new PrintWriter(accountTimeFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        positionTimeFile = new File("/tmp/positionTime");
        try {
            positionTime = new PrintWriter(positionTimeFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple input) {
        String record = (String) input.getValue(0);
        if (record.startsWith("0")) {
            Long startTime = System.currentTimeMillis();
            String[] values = record.split(",");
            String minute = Integer.toString((int) (Math.floor(Integer.parseInt(values[1]) / 60) + 1));
            String vehicleID = values[2];
            String speed = values[3];
            String xWay = values[4];
            String lane = values[5];
            String direction = values[6];
            String segment = values[7];
            String position = values[8];
            String queryId = values[9];
            positionReportWriter.println("Processing: " + vehicleID + " at time " + values[1]);
            positionReportWriter.flush();

            if(lane.equals("4")) return;

            this.positionReportTable.createPositionReportTable(vehicleID, minute, speed, xWay, lane, direction, segment, position);
            this.averageSpeedTable.writeAverageSpeedOfVehicle(speed, vehicleID, minute);
            this.vehicleInLastFiveMinsTable.writeVehicleList(xWay, lane, direction, segment, vehicleID, minute);
            float tollCost = this.vehicleInLastFiveMinsTable.getTollCost(xWay, lane, direction, segment, vehicleID, minute);
            this.accountBalanceTable.updateAccountBalance(vehicleID, minute, queryId, tollCost);

            Long endTime = System.currentTimeMillis();
            Long time = endTime - startTime;
            positionTime.println(time);
            positionTime.flush();

        } else if(record.startsWith("2")) {
            Long startTime = System.currentTimeMillis();
            String[] values = record.split(",");
            String vehicleID = values[2];
            Key vehicleAccountBalanceKey = new Key(Constants.AS_NAMESPACE, Constants.AS_ACCOUNT_BALANCE_SET, vehicleID);
            if (this.client.exists(this.writePolicy,vehicleAccountBalanceKey)) {
                Record abRecord = this.client.get(this.writePolicy, vehicleAccountBalanceKey, Constants.ACCOUNT_BALANCE_BIN);
                String ab = abRecord.getValue(Constants.ACCOUNT_BALANCE_BIN).toString();
                accountBalanceWriter.println("Account Balance: Time: " + values[1] + ", " + vehicleID + ": " + ab);

            } else {
                accountBalanceWriter.println("Account Balance: Time: " + values[1] + ", " + vehicleID + ":0");
            }
            accountBalanceWriter.flush();
            Long endTime = System.currentTimeMillis();
            Long time = endTime - startTime;
            accountTime.println(time);
            accountTime.flush();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
