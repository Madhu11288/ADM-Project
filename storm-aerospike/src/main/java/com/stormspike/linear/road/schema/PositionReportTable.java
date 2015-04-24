package com.stormspike.linear.road.schema;


import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.stormspike.topology.Constants;

public class PositionReportTable {

    private WritePolicy writePolicy;
    private AerospikeClient client;

    public PositionReportTable() {
        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();
    }

    public static void main(String[] args) {
        PositionReportTable nc = new PositionReportTable();
        nc.createPositionReportTable();
        nc.createAccountBalanceTable();
    }

    public void createPositionReportTable() {
            Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_POSITION_REPORT_SET, 108 );
            Bin bin0 = new Bin(Constants.VEHICLE_ID_BIN, 108 );
            Bin bin1 = new Bin(Constants.TIME_BIN, 0);
            Bin bin2 = new Bin(Constants.SPEED_BIN, 23);
            Bin bin3 = new Bin(Constants.XWAY_BIN, 0);
            Bin bin4 = new Bin(Constants.LANE_BIN, 0);
            Bin bin5 = new Bin(Constants.DIRECTION_BIN, 0);
            Bin bin6 = new Bin(Constants.SEGMENT_BIN, 0);
            Bin bin7 = new Bin(Constants.POSITION_BIN, 374 );
            this.client.put(writePolicy, key, bin0, bin1, bin2, bin3, bin4, bin5, bin6, bin7);

    }

    public void createAccountBalanceTable() {
        int vehicle_id = 1;
        int time = 0;
        int key_id;
        key_id = vehicle_id + time;
        Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_ACCOUNT_BALANCE_SET, key_id);
        Bin bin0 = new Bin(Constants.VEHICLE_ID_BIN, 1);
        Bin bin1 = new Bin(Constants.TIME_BIN, 0);
        Bin bin2 = new Bin(Constants.QUERY_ID_BIN, 116);
        this.client.put(writePolicy, key, bin0, bin1, bin2);
    }

}

