package com.stormspike.linear.road.schema;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.stormspike.topology.Constants;

import java.util.ArrayList;

public class VehicleInLastFiveMinsTable {

    private WritePolicy writePolicy;
    private AerospikeClient client;

    public VehicleInLastFiveMinsTable() {
        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();
    }

    public void writeVehicleList(String xWay, String lane, String direction, String segment, String vehicleId,String time) {
        IndexTask indexTask = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "vehicleid-Vlist",Constants.VEHICLE_ID_BIN, IndexType.STRING);
        indexTask.waitTillComplete();

        IndexTask indexTask1 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "xWay",Constants.XWAY_BIN, IndexType.STRING);
        indexTask1.waitTillComplete();

        IndexTask indexTask2 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "lane",Constants.LANE_BIN, IndexType.STRING);
        indexTask2.waitTillComplete();

        IndexTask indexTask3 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "direction",Constants.DIRECTION_BIN, IndexType.STRING);
        indexTask3.waitTillComplete();

        IndexTask indexTask4 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "segment",Constants.SEGMENT_BIN, IndexType.STRING);
        indexTask4.waitTillComplete();

        IndexTask indexTask5 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "time",Constants.TIME_BIN, IndexType.STRING);
        indexTask5.waitTillComplete();

        String keyId = xWay + lane + direction + segment + time;
        Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, keyId);
        Bin bin1 = new Bin(Constants.XWAY_BIN, xWay);
        Bin bin2 = new Bin(Constants.LANE_BIN, lane);
        Bin bin3 = new Bin(Constants.DIRECTION_BIN, direction);
        Bin bin4 = new Bin(Constants.SEGMENT_BIN, segment);
        Bin bin5 = new Bin(Constants.TIME_BIN, time);
        Bin bin6;

        Record record = this.client.get(this.writePolicy, new Key(Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, Value.get(keyId)), Constants.VEHICLE_LIST_BIN);
        if(record!=null) {
            ArrayList<String> vehicleList = (ArrayList) record.getValue(Constants.VEHICLE_LIST_BIN);
            if (!vehicleList.contains(vehicleId)) {
                vehicleList.add(vehicleId);
            }
            bin6 = new Bin(Constants.VEHICLE_LIST_BIN, Value.get(vehicleList));
            this.client.put(this.writePolicy, key, bin1, bin2, bin3, bin4, bin5, bin6);
        } else{
            ArrayList<String> vehicleList = new ArrayList<String>();
            vehicleList.add(vehicleId);
            bin6 = new Bin(Constants.VEHICLE_LIST_BIN,Value.get(vehicleList));
            this.client.put(writePolicy, key,  bin1, bin2, bin3, bin4, bin5, bin6);
        }
    }

    public float getTollCost(String xWay, String lane, String direction, String segment, String vehicleID, String minute) {
        Float toll = (float) 0.0;
        String keyId = xWay + lane + direction + segment + minute;
        Record record = this.client.get(this.writePolicy, new Key(Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, Value.get(keyId)), Constants.VEHICLE_LIST_BIN);

        if (record != null) {
            ArrayList vehicleList = (ArrayList) record.getValue(Constants.VEHICLE_LIST_BIN);
            int size = vehicleList.size();
            ArrayList<Float> avgSpeed = new ArrayList<>();
            float totalAvgSpeed = (float) 0.0;
            if (size > 50) {
                for (int i = 1; i <= 5; i++) {
                    Integer time = Integer.parseInt(minute) - i;
                    String segmentKey = xWay + lane + direction + segment + time;
                    Record segmentRecord = this.client.get(this.writePolicy, new Key(Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, Value.get(segmentKey)), Constants.VEHICLE_LIST_BIN);
                    ArrayList<String> segmentVehicleList = (ArrayList) segmentRecord.getValue(Constants.VEHICLE_LIST_BIN);
                    float curentMinuteAvgSpeed = (float) 0.0;
                    for (String vehicleId : segmentVehicleList) {
                        String avgSpeedKeyId = vehicleId + String.valueOf(time);
                        Record avgSpeedRecord = this.client.get(this.writePolicy, new Key(Constants.AS_NAMESPACE, Constants.AS_AVG_SPEED_SET, Value.get(avgSpeedKeyId)), Constants.AVERAGE_SPEED_BIN);
                        curentMinuteAvgSpeed += Float.parseFloat(avgSpeedRecord.getValue(Constants.AVERAGE_SPEED_BIN).toString());
                    }
                    curentMinuteAvgSpeed = curentMinuteAvgSpeed / segmentVehicleList.size();
                    avgSpeed.add(curentMinuteAvgSpeed);
                }
                for (int i = 0; i < 5; i++) {
                    totalAvgSpeed += avgSpeed.get(i);
                }
                totalAvgSpeed = totalAvgSpeed / 5;
                if (totalAvgSpeed < 40) {
                    toll = (float) (2 * Math.pow((size - 50), 2));
                }
            }
        }
        return toll;
    }
}
