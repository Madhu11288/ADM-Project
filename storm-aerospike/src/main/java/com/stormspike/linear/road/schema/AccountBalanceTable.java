package com.stormspike.linear.road.schema;


import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.stormspike.topology.Constants;

public class AccountBalanceTable {

    private WritePolicy writePolicy;
    private AerospikeClient client;

    public AccountBalanceTable() {
        client = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        writePolicy = new WritePolicy();
    }

    public void createAccountBalanceTable(String vehicleId,String time,String queryId) {
        IndexTask indexTask = this.client.createIndex(writePolicy, Constants.AS_NAMESPACE, Constants.AS_ACCOUNT_BALANCE_SET, "vehicleid-AB",Constants.VEHICLE_ID_BIN, IndexType.STRING);
        indexTask.waitTillComplete();

        Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_ACCOUNT_BALANCE_SET, vehicleId);
        Bin bin0 = new Bin(Constants.VEHICLE_ID_BIN, vehicleId);
        Bin bin1 = new Bin(Constants.TIME_BIN, time);
        Bin bin2 = new Bin(Constants.QUERY_ID_BIN, queryId);
        Bin bin3 = new Bin(Constants.ACCOUNT_BALANCE_BIN,0);
        this.client.put(writePolicy, key, bin0, bin1, bin2, bin3);
    }

    public void updateAccountBalance(String vehicleID,String time, String queryId, float tollCost) {
        float accountBalance;
        Key key = new Key(Constants.AS_NAMESPACE, Constants.AS_ACCOUNT_BALANCE_SET, vehicleID);
        if (this.client.exists(this.writePolicy, key)) {
            Record record = this.client.get(this.writePolicy, key, Constants.ACCOUNT_BALANCE_BIN);
            accountBalance = Float.parseFloat(record.getValue(Constants.ACCOUNT_BALANCE_BIN).toString()) + tollCost;
            Bin bin0 = new Bin(Constants.VEHICLE_ID_BIN, vehicleID);
            Bin bin1 = new Bin(Constants.TIME_BIN, time);
            Bin bin2 = new Bin(Constants.QUERY_ID_BIN, queryId);
            Bin bin3 = new Bin(Constants.ACCOUNT_BALANCE_BIN, accountBalance);
            this.client.put(writePolicy, key, bin0,bin1,bin2,bin3);
        } else{
            createAccountBalanceTable(vehicleID,time,queryId);
        }
    }
}
