package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.*;

import java.util.*;

public class PositionReportBolt implements IRichBolt{

    OutputCollector outputCollector;
//    JedisCluster jedis;
    Jedis jedis;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.jedis = new Jedis("localhost");
//        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7000));
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7001));
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7002));
//        jedis = new JedisCluster(jedisClusterNodes);

    }

    @Override
    public void execute(Tuple input) {

            String record = (String) input.getValue(0);
            String[] values = record.split(",");
            String minute = Integer.toString((int) (Math.floor(Integer.parseInt(values[1])/60) + 1));
            String vehicleID = values[2];
            String speed = values[3];
            String xWay = values[4];
            String lane = values[5];
            String direction = values[6];
            String segment = values[7];
            String position = values[8];

            System.out.println("Processing: " + vehicleID + " at time " + values[1]);

            String vehiclePositionReportKey = "position-report:vehicle-id:" + vehicleID;
            HashMap<String, String> positionReport = new HashMap<String, String>();
            positionReport.put("minute", minute);
            positionReport.put("speed", speed);
            positionReport.put("xWay", xWay);
            positionReport.put("lane", lane);
            positionReport.put("direction", direction);
            positionReport.put("segment", segment);
            positionReport.put("position", position);

            String vehicleAvgSpeedKey = "avg-speed:minute:" + minute + ":vehicle-id:" + vehicleID;
            String vehicleAccountBalanceKey = "account-balance:vehicle-id:" + vehicleID;
            String vehiclesCurrentSegmentKey = "report:minute:" + minute + ":xWay:" + xWay + ":lane:" + lane + ":direction:"
                    + direction + ":segment:" + segment;


            if (!jedis.exists(vehicleAvgSpeedKey)) {
                jedis.set(vehicleAvgSpeedKey, speed);
                jedis.expire(vehicleAvgSpeedKey, 600);
            } else {
                Float avgSpeed = (Float.parseFloat(jedis.get(vehicleAvgSpeedKey)) + Float.parseFloat(speed))/2;
                jedis.set(vehicleAvgSpeedKey, avgSpeed.toString());
            }

            if (!jedis.exists(vehiclePositionReportKey)) {
                jedis.hmset(vehiclePositionReportKey, positionReport);
                jedis.incrBy(vehicleAccountBalanceKey, 0);
            } else {
                List<String> storedValues = jedis.hmget(vehiclePositionReportKey,
                        "xWay", "lane", "direction", "segment", "minute");

                if (!storedValues.get(0).equals(xWay) || !storedValues.get(1).equals(lane)
                        || !storedValues.get(2).equals(direction) || !storedValues.get(3).equals(segment)) {

                    ArrayList<String> segmentKeys = new ArrayList<String>();
                    for(int i = 1; i <= 5; i++) {
                        Integer time = Integer.parseInt(minute) - i;
                        String segmentKey = "report:minute:" + time + ":xWay:" + xWay + ":lane:" + lane + ":direction:"
                                + direction + ":segment:" + segment;
                        segmentKeys.add(segmentKey);
                    }

                    List<String> vehicleIdsForLastMinute = jedis.lrange(segmentKeys.get(0), 0, -1);
                    if (vehicleIdsForLastMinute.size() > 50) {
                        Float avgSpeedForFiveMinutes = new Float(0.0);
                        for (String segmentKey : segmentKeys) {
                            List<String> vehicleIds = jedis.lrange(segmentKey, 0, -1);
                            System.out.println(vehicleIds);
                            Float avgSpeedForThisMinute = new Float(0.0);
                            for (String vehicleId : vehicleIds) {
                                avgSpeedForThisMinute += Float.parseFloat(jedis.get(vehicleId));
                            }

                            if (vehicleIds.size() > 0) {
                                avgSpeedForFiveMinutes += (avgSpeedForThisMinute / vehicleIds.size());
                            }
                        }

                        if (avgSpeedForFiveMinutes < 40) {
                            System.out.println("Tolling for Vehicle: " + vehicleID);
                            Integer toll = (int) (2 * Math.pow((vehicleIdsForLastMinute.size() - 50), 2));
                            jedis.incrBy(vehicleAccountBalanceKey, toll);
                        }
                    }
                }

                jedis.hmset(vehiclePositionReportKey, positionReport);
            }

            if (!jedis.exists(vehiclesCurrentSegmentKey)) {
                jedis.lpush(vehiclesCurrentSegmentKey, vehicleAvgSpeedKey);
                jedis.expire(vehiclesCurrentSegmentKey, 600);
            } else {
                jedis.lpush(vehiclesCurrentSegmentKey, vehicleAvgSpeedKey);
            }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
