package com.stormspike.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.stormspike.linear.road.schema.AccountBalanceTable;
import com.stormspike.linear.road.schema.AverageSpeedTable;
import com.stormspike.linear.road.schema.PositionReportTable;
import com.stormspike.linear.road.schema.VehicleInLastFiveMinsTable;

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

    File file;
    PrintWriter writer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.positionReportTable = new PositionReportTable();
        this.averageSpeedTable = new AverageSpeedTable();
        this.vehicleInLastFiveMinsTable = new VehicleInLastFiveMinsTable();
        this.accountBalanceTable = new AccountBalanceTable();

        file = new File("/Users/madhushrees/ADM_/codeBase/ADM-Project/storm-aerospike/src/main/java/com/stormspike/results/position_reports.txt");
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
        String minute = values[1];
        String vehicleID = values[2];
        String speed = values[3];
        String xWay = values[4];
        String lane = values[5];
        String direction = values[6];
        String segment = values[7];
        String position = values[8];
        String queryId = values[9];

        synchronized (this) {
            writer.println("Processing: " + vehicleID + " at time " + values[1]);
            writer.flush();
        }

        this.positionReportTable.createPositionReportTable(vehicleID, minute, speed, xWay, lane, direction, segment, position);
        this.averageSpeedTable.writeAverageSpeedOfVehicle(speed, vehicleID, minute);
        this.vehicleInLastFiveMinsTable.writeVehicleList(xWay, lane, direction, segment, vehicleID, minute);
        float tollCost = this.vehicleInLastFiveMinsTable.getTollCost(xWay, lane, direction, segment, vehicleID, minute);
        this.accountBalanceTable.updateAccountBalance(vehicleID, minute, queryId, tollCost);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
