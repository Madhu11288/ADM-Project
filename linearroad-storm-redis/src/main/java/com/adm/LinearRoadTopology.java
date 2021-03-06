package com.adm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class LinearRoadTopology {
    public static void main(String[] args) {
        LinearRoadTopology linearRoadTopology = new LinearRoadTopology();
        linearRoadTopology.setUpAndRunTopology();
    }

    private void setUpAndRunTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("linear-road-PR", new LinearRoadCommonSpout(), 1).setNumTasks(1);
//        topologyBuilder.setSpout("linear-road-AB", new LinearRoadABSpout(), 1);
//        topologyBuilder.setBolt("splitterBolt", new SplitterBolt(), 1).shuffleGrouping("linear-road-PR");
          topologyBuilder.setBolt("query", new PositionReportBolt(), 1).shuffleGrouping("linear-road-PR");
//        topologyBuilder.setBolt("query-2-AB", new AccountBalanceBolt(), 1).shuffleGrouping("splitterBolt", "accountBalanceStream");

        Config conf = new Config();
        conf.put(Config.NIMBUS_HOST, "localhost"); //YOUR NIMBUS'S IP
        conf.put(Config.NIMBUS_THRIFT_PORT,6627);    //int is expected here
        conf.setNumWorkers(4);
        try {
            StormSubmitter.submitTopology("test", conf, topologyBuilder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
