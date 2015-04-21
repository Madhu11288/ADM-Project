package com.adm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class LinearRoadTopology {
    public static void main(String[] args) {
        LinearRoadTopology linearRoadTopology = new LinearRoadTopology();
        linearRoadTopology.setUpAndRunTopology();
    }

    private void setUpAndRunTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("linear-road", new LinearRoadSpout(), 1);
        topologyBuilder.setBolt("data-forwarder", new ForwarderBolt(), 1).shuffleGrouping("linear-road");
        topologyBuilder.setBolt("query-0-PR", new QueryZeroBoltPositionReport(), 1).fieldsGrouping("data-forwarder",
                new Fields("query-type"));
        topologyBuilder.setBolt("query-0-LC", new CaptureQueryZeroBolt(), 1).fieldsGrouping("data-forwarder",
                new Fields("query-type"));
//        topologyBuilder.setBolt("query-2", new CalculatorBolt(), 1).shuffleGrouping("LinearRoadSpout");
//        topologyBuilder.setBolt("query-3", new CalculatorBolt(), 1).shuffleGrouping("LinearRoadSpout");
//        topologyBuilder.setBolt("query-4", new CalculatorBolt(), 1).shuffleGrouping("LinearRoadSpout");


        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topologyBuilder.createTopology());
    }
}
