package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class PositionReportBolt implements IRichBolt {

    OutputCollector outputCollector;
    //    JedisCluster jedis;
    Jedis jedis;
    File positionFile;
    File accountFile;
    File accountTimeFile, positionTimeFile;
    File timeDiffFile;
    PrintWriter positionWriter;
    PrintWriter accountWriter;
    PrintWriter accountTime;
    PrintWriter positionTime;
    PrintWriter timeDiff;
    long systemStartTime = -1;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.jedis = new Jedis("localhost");
//        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7000));
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7001));
//        jedisClusterNodes.add(new HostAndPort("10.0.0.30", 7002));
//        jedis = new JedisCluster(jedisClusterNodes);
        positionFile = new File("/tmp/positionReport");
        try {
            positionWriter = new PrintWriter(positionFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        accountFile = new File("/tmp/accountBalance");
        try {
            accountWriter = new PrintWriter(accountFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        accountTimeFile = new File("/tmp/accountTime");
        try {
            accountTime = new PrintWriter(accountTimeFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        positionTimeFile = new File("/tmp/positionFile");
        try {
            positionTime = new PrintWriter(positionTimeFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        timeDiffFile = new File("/tmp/timeDiffFile");
        try {
            timeDiff = new PrintWriter(timeDiffFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        if(systemStartTime <= 0) {
            if (systemStartTime == -1) {
                systemStartTime = 0;
            } else {
                systemStartTime = System.currentTimeMillis();
            }
        }

        String record = (String) input.getValue(0);
        if (record.startsWith("0")) {
            Long startTime = System.currentTimeMillis();
            String[] values = record.split(",");
            String minute = Integer.toString((int) (Math.floor(Integer.parseInt(values[1]) / 60) + 1));
            String vehicleID = values[2];
            String speed = values[3];
            String xWay = values[4];
            String lane = values[5];
            String direction = values[6];
            String segment = values[7];
            String position = values[8];
            positionWriter.println("Processing: " + vehicleID + " at time " + values[1]);
            positionWriter.flush();

            if(lane.equals("4")) return;

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
                Float avgSpeed = (Float.parseFloat(jedis.get(vehicleAvgSpeedKey)) + Float.parseFloat(speed)) / 2;
                jedis.set(vehicleAvgSpeedKey, avgSpeed.toString());
            }

            if (!jedis.exists(vehiclePositionReportKey)) {
                jedis.hmset(vehiclePositionReportKey, positionReport);
                jedis.incrBy(vehicleAccountBalanceKey, 0);
                jedis.set(vehicleAccountBalanceKey + ":time", values[1]);
            } else {
                jedis.set(vehicleAccountBalanceKey + ":time", values[1]);
                List<String> storedValues = jedis.hmget(vehiclePositionReportKey,
                        "xWay", "lane", "direction", "segment", "minute");

                if (!storedValues.get(0).equals(xWay) || !storedValues.get(1).equals(lane)
                        || !storedValues.get(2).equals(direction) || !storedValues.get(3).equals(segment)) {

                    ArrayList<String> segmentKeys = new ArrayList<String>();
                    for (int i = 1; i <= 5; i++) {
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
                            //writer.println(vehicleIds);
                            //writer.flush();
                            Float avgSpeedForThisMinute = new Float(0.0);
                            for (String vehicleId : vehicleIds) {
                                avgSpeedForThisMinute += Float.parseFloat(jedis.get(vehicleId));
                            }

                            if (vehicleIds.size() > 0) {
                                avgSpeedForFiveMinutes += (avgSpeedForThisMinute / vehicleIds.size());
                            }
                        }

                        if (avgSpeedForFiveMinutes < 40) {
                            //writer.println("Tolling for Vehicle: " + vehicleID);
                            //writer.flush();
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
            Long endTime = System.currentTimeMillis();
            Long time = endTime - startTime;
            positionTime.println(time);
            positionTime.flush();
        } else if (record.startsWith("2")) {
            Long startTime = System.currentTimeMillis();
            String[] values = record.split(",");
            String vehicleID = values[2];
            String vehicleAccountBalanceKey = "account-balance:vehicle-id:" + vehicleID;
            String accountBalanceTime = values[1];
            if(jedis.get(vehicleAccountBalanceKey + ":time") != null)
            {
                accountBalanceTime = jedis.get(vehicleAccountBalanceKey + ":time");
            }
            Long timeOutputted = ((System.currentTimeMillis() - systemStartTime) / 1000);
            timeDiff.println(timeOutputted - Long.parseLong(values[1]));
            timeDiff.flush();
            if (jedis.exists(vehicleAccountBalanceKey)) {
                //accountWriter.println("Account Balance: Time: " + values[1] + ", " + vehicleID + ": " + jedis.get(vehicleAccountBalanceKey));
                accountWriter.println("2," + values[1] + "," + (timeOutputted) + "," + values[9] + "," +
                                      jedis.get(vehicleAccountBalanceKey) + "," +
                                      accountBalanceTime);
            } else {
                accountWriter.println("2," + values[1] + "," + (Integer.parseInt(values[1]) + 1) + "," + values[9] + "," +
                        0 + "," +
                        accountBalanceTime);
            }
            accountWriter.flush();
            Long endTime = System.currentTimeMillis();
            Long time = endTime - startTime;
            accountTime.println(time);
            accountTime.flush();
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
