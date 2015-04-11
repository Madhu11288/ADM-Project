package com.stormspike.bolt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.RecordExistsAction;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class AerospikeBolt extends BaseRichBolt {

    private OutputCollector collector;

    private AerospikeClient aerospikeClient;
    private WritePolicy aerospikeWritePolicy;

    private static String AEROSPIKE_NS;
    private static String AEROSPIKE_SET;
    public static String AEROSPIKE_HOST = "127.0.0.1";
    public static int AEROSPIKE_PORT = 3000;

    private static final String TWEET_ID_BIN = "tweetId_bin";
    private static final String TWEET_DATE_BIN = "tweetDate";
    private static final String TWEET_LOCATION_BIN = "tweetLocation";
    private static final String TWEET_TEXT_BIN = "tweetText";
    private static final String USER_NAME_BIN = "userName";
    private static final String TWEET_HASHTAG_BIN = "hashTag";
    private static final String TWEET_ID = "tweetid";
    private static final String USER_NAME = "username";



    public AerospikeBolt(String aerospikeHost, int aerospikePort, String AEROSPIKE_NS, String aerospikeSet) {
        this.AEROSPIKE_HOST = aerospikeHost;
        this.AEROSPIKE_PORT = aerospikePort;
        this.AEROSPIKE_NS = AEROSPIKE_NS;
        this.AEROSPIKE_SET = aerospikeSet;

    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
        try {
            this.aerospikeClient = new AerospikeClient(this.AEROSPIKE_HOST, this.AEROSPIKE_PORT);
            this.aerospikeWritePolicy = new WritePolicy();
            this.aerospikeWritePolicy.maxRetries=10;
            this.aerospikeWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;


            IndexTask indexTask1 = this.aerospikeClient.createIndex(aerospikeWritePolicy, AEROSPIKE_NS, AEROSPIKE_SET, TWEET_ID, TWEET_ID_BIN, IndexType.NUMERIC);
            indexTask1.waitTillComplete();

            IndexTask indexTask2 = this.aerospikeClient.createIndex(aerospikeWritePolicy, AEROSPIKE_NS, AEROSPIKE_SET, USER_NAME, USER_NAME_BIN, IndexType.STRING);
            indexTask2.waitTillComplete();

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



        Key key = new Key(AEROSPIKE_NS, AEROSPIKE_SET, status.getId());

        Bin bin1 = new Bin(USER_NAME_BIN,status.getUser().getName());
        Bin bin2 = new Bin(TWEET_TEXT_BIN,status.getText());
        Bin bin3 = new Bin(TWEET_LOCATION_BIN, status.getGeoLocation());
        Bin bin4 = new Bin(TWEET_DATE_BIN,status.getCreatedAt().toString());
        Bin bin5 = new Bin(TWEET_ID_BIN,status.getId());

        this.aerospikeClient.put(this.aerospikeWritePolicy, key, bin1, bin2, bin3, bin4, bin5);


        //LargeList largeList = new LargeList();

        LargeList largeList = this.aerospikeClient.getLargeList(this.aerospikeWritePolicy, key, TWEET_HASHTAG_BIN, null);
        HashtagEntity[] htEntity = status.getHashtagEntities();

        //create a set to hold the hashtags so that duplicate hashtags inthe same tweet is eliminated
        Set<String> hashtagSet = new HashSet<>();

        if(htEntity.length > 0) {
            for (HashtagEntity ht : htEntity) {
                System.out.println("ht.getText() = " + ht.getText());
                hashtagSet.add(ht.getText());
            }

            for(String hashTags : hashtagSet){
                largeList.add(Value.get(hashTags));
            }
        }

        Bin bin6 = new Bin(TWEET_HASHTAG_BIN, largeList);
        this.aerospikeClient.put(this.aerospikeWritePolicy, key, bin6);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
