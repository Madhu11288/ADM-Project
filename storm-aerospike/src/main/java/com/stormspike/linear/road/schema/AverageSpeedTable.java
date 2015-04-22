package com.stormspike.linear.road.schema;


import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.stormspike.topology.Constants;

public class AverageSpeedTable {

    private  WritePolicy writePolicy;
    private AerospikeClient client;

    public AverageSpeedTable(){
        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();
    }

    public static void main(String[] args) {
        AverageSpeedTable avgs = new AverageSpeedTable();
        avgs.writeAverageSpeedOfVehicle();
    }

    public void writeAverageSpeedOfVehicle(){
        float curentSpeed = 2;
        float averageSpeed = 0;
        int  vehicle_id = 111;
        int time = 0;
        String key_id ;
        key_id  = String.valueOf((vehicle_id + time));
        Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_AVG_SPEED_SET, key_id);
        Bin bin0 = new Bin(Constants.VEHICLE_ID_BIN, vehicle_id);
        Bin bin1 = new Bin(Constants.TIME_BIN, time);
        Bin bin2;
        Record record = this.client.get(this.writePolicy, new Key(Constants.AS_NAMESPACE, Constants.AS_AVG_SPEED_SET, Value.get(key_id)), Constants.AVERAGE_SPEED_BIN);
        if(record!=null){
            averageSpeed =  ((Float.parseFloat(record.getValue(Constants.AVERAGE_SPEED_BIN).toString())) + curentSpeed ) / 2;
            bin2 = new Bin(Constants.AVERAGE_SPEED_BIN, Value.get(String.valueOf(averageSpeed)));
            this.client.put(this.writePolicy,key,bin0,bin1,bin2);
        } else {
            bin2 = new Bin(Constants.AVERAGE_SPEED_BIN, Value.get(String.valueOf(curentSpeed)));
            this.client.put(this.writePolicy,key,bin0,bin1,bin2);
        }
    }


}
