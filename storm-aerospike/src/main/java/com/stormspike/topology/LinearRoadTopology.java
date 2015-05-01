package com.stormspike.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.stormspike.bolt.AccountBalanceBolt;
import com.stormspike.bolt.PositionReportBolt;
import com.stormspike.bolt.SplitterBolt;
import com.stormspike.spout.LinearRoadSpout;

public class LinearRoadTopology {
    public static void main(String[] args) {
        LinearRoadTopology linearRoadTopology = new LinearRoadTopology();
        linearRoadTopology.setUpAndRunTopology();
    }

    private void setUpAndRunTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("linear-road-PR", new LinearRoadSpout(), 1).setNumTasks(1);
        //topologyBuilder.setSpout("linear-road-AB", new LinearRoadABSpout(), 1);
        topologyBuilder.setBolt("splitterBolt", new SplitterBolt(), 1).shuffleGrouping("linear-road-PR");
        topologyBuilder.setBolt("query-0-PR", new PositionReportBolt(), 1).shuffleGrouping("splitterBolt", "positionReportStream").setNumTasks(1);
        topologyBuilder.setBolt("query-2-AB", new AccountBalanceBolt(), 1).shuffleGrouping("splitterBolt", "accountBalanceStream");

        Config conf = new Config();
        conf.put(Config.NIMBUS_HOST, "localhost"); //YOUR NIMBUS'S IP
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);    //int is expected here
        conf.setNumWorkers(1);
        try {
            StormSubmitter.submitTopology("test", conf, topologyBuilder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}