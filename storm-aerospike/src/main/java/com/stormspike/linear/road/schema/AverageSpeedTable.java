package com.stormspike.linear.road.schema;


import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.stormspike.topology.Constants;

public class AverageSpeedTable {

    private  WritePolicy writePolicy;
    private AerospikeClient client;

    public AverageSpeedTable(){
        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();
    }

    public void writeAverageSpeedOfVehicle(String currentSpeed,String vehicleId, String time){
        IndexTask indexTask = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_AVG_SPEED_SET, "vehicleid-ASpeed",Constants.VEHICLE_ID_BIN, IndexType.STRING);
        indexTask.waitTillComplete();

        float averageSpeed = 0;
        String key_id ;
        key_id  = String.valueOf((vehicleId + time));
        Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_AVG_SPEED_SET, key_id);
        Bin bin0 = new Bin(Constants.VEHICLE_ID_BIN, vehicleId);
        Bin bin1 = new Bin(Constants.TIME_BIN, time);
        Bin bin2;
        Record record = this.client.get(this.writePolicy, new Key(Constants.AS_NAMESPACE, Constants.AS_AVG_SPEED_SET, Value.get(key_id)), Constants.AVERAGE_SPEED_BIN);
        if(record!=null){
            averageSpeed =  ((Float.parseFloat(record.getValue(Constants.AVERAGE_SPEED_BIN).toString())) + Float.parseFloat(currentSpeed) ) / 2;
            bin2 = new Bin(Constants.AVERAGE_SPEED_BIN, Value.get(String.valueOf(averageSpeed)));
            this.client.put(this.writePolicy,key,bin0,bin1,bin2);
        } else {
            bin2 = new Bin(Constants.AVERAGE_SPEED_BIN, Value.get(String.valueOf(currentSpeed)));
            this.client.put(this.writePolicy,key,bin0,bin1,bin2);
        }
    }

}
