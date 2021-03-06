package com.adm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class AerospikeTweetsBolt extends BaseRichBolt {

    private OutputCollector collector;

    private AerospikeClient aerospikeClient;
    private WritePolicy aerospikeWritePolicy;

    private static final String TWEET_ID_BIN = "tweetId_bin";
    private static final String TWEET_DATETIME_BIN = "tweetDateTime";
    private static final String TWEET_LOCATION_BIN = "tweetLocation";
    private static final String TWEET_TEXT_BIN = "tweetText";
    private static final String USER_NAME_BIN = "userName";
    private static final String TWEET_HASHTAG_BIN = "hashTag";
    private static final String RETWEET_BIN = "retweet";
    private static final String TWEET_ID = "tweetid";
    private static final String USER_NAME = "username";
    private String namespace;
    private String set;

    public AerospikeTweetsBolt(String namespace, String set) {
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


            IndexTask indexTask1 = this.aerospikeClient.createIndex(aerospikeWritePolicy, this.namespace, this.set, USER_NAME, USER_NAME_BIN, IndexType.STRING);
            indexTask1.waitTillComplete();

            IndexTask indexTask2 = this.aerospikeClient.createIndex(aerospikeWritePolicy, this.namespace, this.set, TWEET_HASHTAG_BIN, TWEET_HASHTAG_BIN, IndexType.STRING);
            indexTask2.waitTillComplete();

            IndexTask indexTask3 = this.aerospikeClient.createIndex(aerospikeWritePolicy, this.namespace, this.set, TWEET_DATETIME_BIN, TWEET_DATETIME_BIN, IndexType.NUMERIC);
            indexTask3.waitTillComplete();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {

        // First we need to get our key value
        Status status = (Status) input.getValueByField("tweet");
        Key key = new Key(this.namespace, this.set, status.getId());

        String geoLoc = new String("");
        if(status.getGeoLocation() != null){
            geoLoc = status.getGeoLocation().toString();
        }

        Bin bin0 = new Bin(TWEET_ID_BIN, Value.get(status.getId()));
        Bin bin1 = new Bin(USER_NAME_BIN, Value.get(status.getUser().getName()));
        Bin bin2 = new Bin(TWEET_TEXT_BIN, Value.get(status.getText().replaceAll("\n", " ")));
        Bin bin3 = new Bin(TWEET_LOCATION_BIN, Value.get(geoLoc));
        //Bin bin4 = new Bin(TWEET_DATETIME_BIN, Value.get(status.getCreatedAt().toString()));

        Date date = status.getCreatedAt();
        long epochTime = date.getTime();
        Bin bin4 = new Bin(TWEET_DATETIME_BIN, Value.get(epochTime));


        HashtagEntity[] htEntity = status.getHashtagEntities();
        //create a set to hold the hashtags so that duplicate hashtags inthe same tweet is eliminated
        List<String> hashTags = new ArrayList<String>();

        if (htEntity.length > 0) {
            for (HashtagEntity ht : htEntity) {
                hashTags.add(ht.getText());
            }
        }
        Bin bin5 = new Bin(TWEET_HASHTAG_BIN, Value.get(hashTags));
        Bin bin6 = new Bin(RETWEET_BIN,Value.get(status.getRetweetCount()));

        this.aerospikeClient.put(this.aerospikeWritePolicy, key, bin0, bin1, bin2, bin3, bin4, bin5, bin6);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
