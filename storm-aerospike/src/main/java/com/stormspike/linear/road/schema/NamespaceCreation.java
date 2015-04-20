package com.stormspike.linear.road.schema;


import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.stormspike.topology.Constants;

public class NamespaceCreation {
    public static String AS_NAMESPACE = "linear_road_bmrk";
    public static String AS_POSITION_REPORT_SET = "position_reports";
    public static String AS_ACCOUNT_BALANCE_SET = "account_balance";

    public static String TIME_BIN = "time";
    public static String QUERY_ID_BIN = "QID";
    public static String SPEED_BIN = "speed";
    public static String XWAY_BIN = "xway";
    public static String LANE_BIN = "lane";
    public static String DIRECTION_BIN = "direction";
    public static String SEGMENT_BIN = "segment";
    public static String POSITION_BIN = "position";
    public static String VEHICLE_ID_BIN = "vehicleID";
    private WritePolicy writePolicy;
    private AerospikeClient client;

    public NamespaceCreation(){
        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();
    }

    public static void main(String[] args) {
        NamespaceCreation nc = new NamespaceCreation();
        nc.createPositionReportTable();
        nc.createAccountBalanceTable();
    }

    public void createPositionReportTable(){
        Key key = new Key(AS_NAMESPACE, AS_POSITION_REPORT_SET, 108);
        Bin bin0 = new Bin(VEHICLE_ID_BIN, 108);
        Bin bin1 = new Bin(TIME_BIN, 0);
        Bin bin2 = new Bin(SPEED_BIN, 23);
        Bin bin3 = new Bin(XWAY_BIN, 0);
        Bin bin4 = new Bin(LANE_BIN, 0);
        Bin bin5 = new Bin(DIRECTION_BIN, 0);
        Bin bin6 = new Bin(SEGMENT_BIN, 0);
        Bin bin7 = new Bin(POSITION_BIN, 374);
        this.client.put(writePolicy, key, bin0, bin1, bin2, bin3, bin4, bin5, bin6,bin7);
    }

    public void createAccountBalanceTable(){
        int vehicle_id = 1;
        int time = 0;
        int key_id;
        key_id = vehicle_id + time;
        Key key = new Key(AS_NAMESPACE, AS_ACCOUNT_BALANCE_SET, key_id);
        Bin bin0 = new Bin(VEHICLE_ID_BIN, 1);
        Bin bin1 = new Bin(TIME_BIN, 0);
        Bin bin2 = new Bin(QUERY_ID_BIN, 116);
        this.client.put(writePolicy, key, bin0, bin1, bin2);
    }
}

