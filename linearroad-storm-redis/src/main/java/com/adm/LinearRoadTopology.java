package com.adm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class LinearRoadTopology {
    public static void main(String[] args) {
        LinearRoadTopology linearRoadTopology = new LinearRoadTopology();
        linearRoadTopology.setUpAndRunTopology();
    }

    private void setUpAndRunTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("linear-road-PR", new LinearRoadPRSpout(), 1);
        topologyBuilder.setSpout("linear-road-AB", new LinearRoadABSpout(), 1);
        topologyBuilder.setBolt("query-0-PR", new PositionReportBolt(), 1).shuffleGrouping("linear-road-PR");
        topologyBuilder.setBolt("query-2-AB", new AccountBalanceBolt(), 1).shuffleGrouping("linear-road-AB");

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topologyBuilder.createTopology());
    }
}
