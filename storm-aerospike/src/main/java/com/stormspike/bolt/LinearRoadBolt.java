package com.stormspike.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.stormspike.linear.road.schema.AccountBalanceTable;
import com.stormspike.linear.road.schema.AverageSpeedTable;
import com.stormspike.linear.road.schema.PositionReportTable;
import com.stormspike.linear.road.schema.VehicleInLastFiveMinsTable;

import java.util.Map;

public class LinearRoadBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    PositionReportTable positionReportTable;
    AverageSpeedTable averageSpeedTable;
    VehicleInLastFiveMinsTable vehicleInLastFiveMinsTable;
    AccountBalanceTable accountBalanceTable;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.positionReportTable = new PositionReportTable();
        this.averageSpeedTable = new AverageSpeedTable();
        this.vehicleInLastFiveMinsTable = new VehicleInLastFiveMinsTable();
        this.accountBalanceTable = new AccountBalanceTable();
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

        if (((String) input.getValue(0)).startsWith("0")) {
            this.positionReportTable.createPositionReportTable(vehicleID, minute, speed, xWay, lane, direction, segment, position);
            this.averageSpeedTable.writeAverageSpeedOfVehicle(speed, vehicleID, minute);
            this.vehicleInLastFiveMinsTable.writeVehicleList(xWay, lane, direction, segment, vehicleID, minute);
        } else if (((String) input.getValue(0)).startsWith("2")) {
            this.accountBalanceTable.createAccountBalanceTable(vehicleID, minute, queryId);
            float tollCost = this.vehicleInLastFiveMinsTable.getTollCost(xWay, lane, direction, segment, vehicleID, minute);
            this.accountBalanceTable.updateAccountBalance(vehicleID, tollCost);

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
