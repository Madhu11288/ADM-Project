package com.stormspike.linear.road.schema;


import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.stormspike.topology.Constants;

import java.util.ArrayList;

public class VehicleInLastFiveMinsTable {

    private WritePolicy writePolicy;
    private AerospikeClient client;

    public VehicleInLastFiveMinsTable() {
        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();
    }

    public static void main(String[] args) {
        VehicleInLastFiveMinsTable vilfm = new VehicleInLastFiveMinsTable();
        vilfm.writeVehicleList();
    }

    private void writeVehicleList() {
        int xway = 1;
        int lane = 1;
        int direction = 1;
        int segment = 1;
        int vehicleId = 10001;
        String keyId = String.valueOf(xway)+String.valueOf(lane)+String.valueOf(direction)+String.valueOf(segment);
        Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, keyId);
        Bin bin1 = new Bin(Constants.XWAY_BIN, 0);
        Bin bin2 = new Bin(Constants.LANE_BIN, 1);
        Bin bin3 = new Bin(Constants.DIRECTION_BIN, 0);
        Bin bin4 = new Bin(Constants.SEGMENT_BIN, 0);
        Bin bin5 = new Bin(Constants.TIME_BIN, 0);
        Bin bin6;

        Record record = this.client.get(this.writePolicy, new Key(Constants.AS_NAMESPACE, Constants.AS_VEHICLE_LIST_SET, Value.get(keyId)), Constants.VEHICLE_LIST_BIN);
        if(record!=null) {
            ArrayList vehicleList = (ArrayList) record.getValue(Constants.VEHICLE_LIST_BIN);
            if (!vehicleList.contains(vehicleId)) {
                vehicleList.add(vehicleId);
            }
            bin6 = new Bin(Constants.VEHICLE_LIST_BIN, Value.get(vehicleList));
            this.client.put(this.writePolicy, key, bin1, bin2, bin3, bin4, bin5,bin6);
        } else{
            ArrayList vehicleList = new ArrayList();
            vehicleList.add(vehicleId);
            bin6 = new Bin(Constants.VEHICLE_LIST_BIN,Value.get(vehicleList));
            this.client.put(writePolicy, key,  bin1, bin2, bin3, bin4, bin5,bin6);
        }
    }

}
