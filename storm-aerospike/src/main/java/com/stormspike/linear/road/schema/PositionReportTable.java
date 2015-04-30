package com.stormspike.linear.road.schema;


import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.stormspike.topology.Constants;

public class PositionReportTable {

    private WritePolicy writePolicy;
    private AerospikeClient client;

    public PositionReportTable() {
        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();
    }

    public void createPositionReportTable(String vehicleId,String time,String speed, String xWay,String lane,String dir,String segment, String position) {
            IndexTask indexTask1 = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_POSITION_REPORT_SET, "vehicleid-PR",Constants.VEHICLE_ID_BIN, IndexType.STRING);
            indexTask1.waitTillComplete();

            Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_POSITION_REPORT_SET, vehicleId );
            Bin bin0 = new Bin(Constants.VEHICLE_ID_BIN, vehicleId );
            Bin bin1 = new Bin(Constants.TIME_BIN, time);
            Bin bin2 = new Bin(Constants.SPEED_BIN, speed);
            Bin bin3 = new Bin(Constants.XWAY_BIN, xWay);
            Bin bin4 = new Bin(Constants.LANE_BIN, lane);
            Bin bin5 = new Bin(Constants.DIRECTION_BIN, dir);
            Bin bin6 = new Bin(Constants.SEGMENT_BIN, segment);
            Bin bin7 = new Bin(Constants.POSITION_BIN, position );
            this.client.put(writePolicy, key, bin0, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
    }



}

