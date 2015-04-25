package com.stormspike.linear.road.schema;


import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
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
        IndexTask indexTask = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "vehicleid1",Constants.VEHICLE_ID_BIN, IndexType.STRING);
        indexTask.waitTillComplete();

        IndexTask indexTask1 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "xWay",Constants.XWAY_BIN, IndexType.STRING);
        indexTask1.waitTillComplete();

        IndexTask indexTask2 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "lane",Constants.LANE_BIN, IndexType.STRING);
        indexTask2.waitTillComplete();

        IndexTask indexTask3 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "direction",Constants.DIRECTION_BIN, IndexType.STRING);
        indexTask3.waitTillComplete();

        IndexTask indexTask4 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, "segment",Constants.SEGMENT_BIN, IndexType.STRING);
        indexTask4.waitTillComplete();

        String keyId = xWay + lane + direction + segment;
        Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, keyId);
        Bin bin1 = new Bin(Constants.XWAY_BIN, xWay);
        Bin bin2 = new Bin(Constants.LANE_BIN, lane);
        Bin bin3 = new Bin(Constants.DIRECTION_BIN, direction);
        Bin bin4 = new Bin(Constants.SEGMENT_BIN, segment);
        Bin bin5 = new Bin(Constants.TIME_BIN, time);
        Bin bin6;

        Record record = this.client.get(this.writePolicy, new Key(Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, Value.get(keyId)), Constants.VEHICLE_LIST_BIN);
        if(record!=null) {
            ArrayList vehicleList = (ArrayList) record.getValue(Constants.VEHICLE_LIST_BIN);
            if (!vehicleList.contains(vehicleId)) {
                vehicleList.add(vehicleId);
            }
            bin6 = new Bin(Constants.VEHICLE_LIST_BIN, Value.get(vehicleList));
            this.client.put(this.writePolicy, key, bin1, bin2, bin3, bin4, bin5, bin6);
        } else{
            ArrayList vehicleList = new ArrayList();
            vehicleList.add(vehicleId);
            bin6 = new Bin(Constants.VEHICLE_LIST_BIN,Value.get(vehicleList));
            this.client.put(writePolicy, key,  bin1, bin2, bin3, bin4, bin5, bin6);
        }
    }
}
