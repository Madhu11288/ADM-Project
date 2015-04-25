package com.stormspike.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.stormspike.linear.road.schema.AverageSpeedTable;
import com.stormspike.linear.road.schema.PositionReportTable;
import com.stormspike.linear.road.schema.VehicleInLastFiveMinsTable;

import java.util.Map;

public class LinearRoadBolt extends BaseRichBolt{

    private OutputCollector outputCollector;
    PositionReportTable positionReportTable;
    AverageSpeedTable averageSpeedTable;
    VehicleInLastFiveMinsTable vehicleInLastFiveMinsTable;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.positionReportTable = new PositionReportTable();
        this.averageSpeedTable = new AverageSpeedTable();
        this.vehicleInLastFiveMinsTable = new VehicleInLastFiveMinsTable();
    }

    @Override
    public void execute(Tuple input) {
        if(input.getValue(0).toString().startsWith("0")) {
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
            this.positionReportTable.createPositionReportTable(vehicleID, minute, speed, xWay, lane, direction, segment, position);
            this.averageSpeedTable.writeAverageSpeedOfVehicle(speed,vehicleID,minute);
            this.vehicleInLastFiveMinsTable.writeVehicleList(xWay,lane,direction,segment,vehicleID,minute);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
