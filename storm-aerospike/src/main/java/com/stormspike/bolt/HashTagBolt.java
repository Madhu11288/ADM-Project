package com.stormspike.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.aerospike.client.*;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.stormspike.topology.Constants;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Map;

public class HashTagBolt extends BaseRichBolt {

    private static final String TWEET_HASHTAG_BIN = "hashTag";
    private static final String HASHTAG = "hashtag";
    private static final String TWEET_COUNT = "hashTagCount";
    private final String namespace;
    private final String set;
    private OutputCollector collector;
    private AerospikeClient aerospikeClient;
    private WritePolicy aerospikeWritePolicy;

    public HashTagBolt(String namespace, String set) {
        this.namespace = namespace;
        this.set = set;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
        try {
            this.aerospikeClient = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
            this.aerospikeWritePolicy = new WritePolicy();
            this.aerospikeWritePolicy.maxRetries = 10;
            this.aerospikeWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;

            IndexTask indexTask = this.aerospikeClient.createIndex(this.aerospikeWritePolicy, this.namespace, this.set, HASHTAG, TWEET_HASHTAG_BIN, IndexType.STRING);
            indexTask.waitTillComplete();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        // First we need to get our key value
        Status status = (Status) input.getValueByField("tweet");
        System.out.println("---------------------------------");
        System.out.println(status.getId());
        System.out.println(status.getUser().getName());

        HashtagEntity[] htEntity = status.getHashtagEntities();
        //create a set to hold the hashtags so that duplicate hashtags inthe same tweet is eliminated
        if (htEntity.length > 0) {
            for (HashtagEntity ht : htEntity) {
                System.out.println("ht.getText() = " + ht.getText());
                Key key = new Key(this.namespace, this.set, Value.get(ht.getText()));
                Record record = this.aerospikeClient.get(this.aerospikeWritePolicy, key);
                Bin bin0 = new Bin(TWEET_HASHTAG_BIN, Value.get(ht.getText()));
                Bin bin1;
                if (record == null) {
                    bin1 = new Bin(TWEET_COUNT, 1);
                } else {
                    bin1 = new Bin(TWEET_COUNT, record.getInt(TWEET_COUNT) + 1);
                }
                this.aerospikeClient.put(this.aerospikeWritePolicy, key, bin0, bin1);
//                FilterOnHashTags filter = new FilterOnHashTags(this.namespace,this.set);
//                Statement statement = filter.aggregate();
//                ResultSet rs = this.aerospikeClient.query(null, statement, Value.get(10));
//
//                while (rs.next()){
//                    List<Map<String, Object>> result =  (List<Map<String, Object>>) rs.getObject();
//                    for (Map<String, Object> element : result){
//                        System.out.println(element);
//                    }
//                }
//                ResultSet rs = this.aerospikeClient.queryAggregate(null, statement, "filter", "count");
//                if (rs.next()) {
//                    Object result = rs.getObject();
//                    System.out.println("*****************************************************************");
//                    System.out.println("Count = " + result);
//                }

            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
