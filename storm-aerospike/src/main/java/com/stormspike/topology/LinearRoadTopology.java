package com.stormspike.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.stormspike.bolt.ForwarderBolt;
import com.stormspike.bolt.LinearRoadBolt;
import com.stormspike.spout.LinearRoadSpout;

public class LinearRoadTopology {
    public static void main(String[] args) {
        LinearRoadTopology linearRoadTopology = new LinearRoadTopology();
        linearRoadTopology.setUpAndRunTopology();
    }

    private void setUpAndRunTopology() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("linear-road", new LinearRoadSpout(), 1);
        topologyBuilder.setBolt("data-forwarder", new LinearRoadBolt(), 1).shuffleGrouping("linear-road");
//        topologyBuilder.setBolt("query-0-PR", new QueryZeroBoltPositionReport(), 1).fieldsGrouping("data-forwarder",
//                new Fields("query-type"));
//        topologyBuilder.setBolt("query-0-LC", new CaptureQueryZeroBolt(), 1).fieldsGrouping("data-forwarder",
//                new Fields("query-type"));

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topologyBuilder.createTopology());
    }
}
