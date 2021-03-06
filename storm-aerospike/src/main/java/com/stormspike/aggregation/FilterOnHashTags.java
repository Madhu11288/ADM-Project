package com.stormspike.aggregation;


import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.RegisterTask;
import com.stormspike.topology.Constants;

import java.util.List;
import java.util.Map;

public class FilterOnHashTags {
    public static final String AEROSPIKE_NS = "test";
    public static final String AEROSPIKE_SET = "stormset-hashtag";
    private static final String TWEET_HASHTAG_BIN = "hashTag";
    private static final String HASHTAG = "hashtag";
    private static AerospikeClient aerospikeClient;
    private static WritePolicy aerospikeWritePolicy;

    private static void aggregate(Statement stmt) {
        ResultSet rs = aerospikeClient.queryAggregate(null, stmt, "toptweets", "top", Value.get(10));
        while (rs.next()) {
            List<Map<String, Object>> result = (List<Map<String, Object>>) rs.getObject();
            for (Map<String, Object> element : result) {
                System.out.println(element);
            }
        }
    }

    public static void getTrendingTopics() {
        aerospikeClient = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        aerospikeWritePolicy = new WritePolicy();
        aerospikeWritePolicy.maxRetries = 10;
        aerospikeWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;

        RegisterTask rt = aerospikeClient.register(null, "udf/toptweets.lua", "toptweets.lua", Language.LUA);
        rt.waitTillComplete();

        scanAggregate();

    }

    private static void scanAggregate() {
        Statement stmt = new Statement();
        stmt.setNamespace(AEROSPIKE_NS);
        stmt.setSetName(AEROSPIKE_SET);
        stmt.setBinNames(TWEET_HASHTAG_BIN, HASHTAG);
        aggregate(stmt);
    }

    public static void main(String args[]){
        getTrendingTopics();
    }
}