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
        topologyBuilder.setSpout("LinearRoadSpout", new LinearRoadSpout(), 1);
        topologyBuilder.setBolt("CalculatorBolt", new CalculatorBolt(), 1).shuffleGrouping("LinearRoadSpout");


        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topologyBuilder.createTopology());
    }
}
