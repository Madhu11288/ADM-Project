package com.stormspike;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.stormspike.bolt.PositionReportBolt;
import com.stormspike.spout.LinearRoadSpout;

public class LinearRoadTopology {
    public static void main(String[] args) {
        LinearRoadTopology linearRoadTopology = new LinearRoadTopology();
        linearRoadTopology.setUpAndRunTopology();
    }

    private void setUpAndRunTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("linear-road-PR", new LinearRoadSpout(), 1);
        topologyBuilder.setBolt("query", new PositionReportBolt(), 1).shuffleGrouping("linear-road-PR");

        Config conf = new Config();
        conf.put(Config.NIMBUS_HOST, "localhost"); //YOUR NIMBUS'S IP
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);    //int is expected here
        conf.setNumWorkers(2);
        try {
            StormSubmitter.submitTopology("test", conf, topologyBuilder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}