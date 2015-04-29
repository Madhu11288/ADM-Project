package com.adm;


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
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Map;

public class AerospikeHashTagBolt extends BaseRichBolt {

    private static final String TWEET_HASHTAG_BIN = "hashTag";
    private static final String HASHTAG = "hashtag";
    private static final String TWEET_COUNT = "hashTagCount";
    private final String namespace;
    private final String set;
    private OutputCollector collector;
    private AerospikeClient aerospikeClient;
    private WritePolicy aerospikeWritePolicy;

    public AerospikeHashTagBolt(String namespace, String set) {
        this.namespace = namespace;
        this.set = set;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
        try {
            this.aerospikeClient = new AerospikeClient(AerospikeConstants.AEROSPIKE_HOST, AerospikeConstants.AEROSPIKE_PORT);
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

        HashtagEntity[] htEntity = status.getHashtagEntities();
        //create a set to hold the hashtags so that duplicate hashtags inthe same tweet is eliminated
        if (htEntity.length > 0) {
            for (HashtagEntity ht : htEntity) {
                Key key = new Key(this.namespace, this.set, Value.get(ht.getText().toLowerCase()));
                Record record = this.aerospikeClient.get(this.aerospikeWritePolicy, key);
                Bin bin0 = new Bin(TWEET_HASHTAG_BIN, Value.get(ht.getText()));
                Bin bin1;
                if (record == null) {
                    bin1 = new Bin(TWEET_COUNT, 1);
                } else {
                    bin1 = new Bin(TWEET_COUNT, record.getInt(TWEET_COUNT) + 1);
                }
                this.aerospikeClient.put(this.aerospikeWritePolicy, key, bin0, bin1);
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
