package com.stormspike.aggregation;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.stormspike.topology.Constants;

/**
 * Created by tvsamartha on 4/13/15.
 */
public class TimeSlidingWindow {

    public static final String AEROSPIKE_NS = "test";
    public static final String AEROSPIKE_STORMSET = "stormset";

    private static final String TWEET_ID_BIN = "tweetId_bin";
    private static final String TWEET_DATETIME_BIN = "tweetDateTime";
    private static final String TWEET_LOCATION_BIN = "tweetLocation";
    private static final String TWEET_TEXT_BIN = "tweetText";
    private static final String USER_NAME_BIN = "userName";
    private static final String TWEET_HASHTAG_BIN = "hashTag";
    private static final String RETWEET_BIN = "retweet";

    private static AerospikeClient aerospikeClient;
    private static WritePolicy aerospikeWritePolicy;

    private static int recordCounter = 0;

    public static void getTweetsOfUser(String userName){
        System.out.println("\nGet Tweets of " + userName);
        aerospikeClient = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        aerospikeWritePolicy = new WritePolicy();
        aerospikeWritePolicy.maxRetries = 10;
        aerospikeWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;

        Statement stmt = new Statement();
        stmt.setNamespace(AEROSPIKE_NS);
        stmt.setSetName(AEROSPIKE_STORMSET);

        stmt.setFilters(Filter.equal(USER_NAME_BIN, userName));

        stmt.setBinNames(TWEET_ID_BIN, USER_NAME_BIN, TWEET_DATETIME_BIN, TWEET_LOCATION_BIN, TWEET_TEXT_BIN, TWEET_HASHTAG_BIN);

        RecordSet rs = aerospikeClient.query(null, stmt);
        recordCounter = 0;

        try {
            while (rs.next()) {
                Key key = rs.getKey();
                Record record = rs.getRecord();
                System.out.println(record.getValue(USER_NAME_BIN));
                System.out.println(record.getValue(TWEET_TEXT_BIN));
                recordCounter++;
            }
        }
        finally {
            rs.close();
            System.out.println("TOTAL RECORDS = " + recordCounter);
            System.out.println("--------------------------------------------------------");
        }

    }


    public static void getTweetsInTimeframe(long startTime, long endTime){
        System.out.println("\nGet Tweets in timeframe " + startTime + " to " + endTime);
        aerospikeClient = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        aerospikeWritePolicy = new WritePolicy();
        aerospikeWritePolicy.maxRetries = 10;
        aerospikeWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;

        Statement stmt = new Statement();
        stmt.setNamespace(AEROSPIKE_NS);
        stmt.setSetName(AEROSPIKE_STORMSET);

        stmt.setFilters(Filter.range(TWEET_DATETIME_BIN, startTime, endTime));

        stmt.setBinNames(TWEET_ID_BIN, USER_NAME_BIN, TWEET_DATETIME_BIN, TWEET_LOCATION_BIN, TWEET_TEXT_BIN, TWEET_HASHTAG_BIN);

        RecordSet rs = aerospikeClient.query(null, stmt);

        try {
            while (rs.next()) {
                Key key = rs.getKey();
                Record record = rs.getRecord();
                System.out.println(record.getValue(USER_NAME_BIN));
                System.out.println(record.getValue(TWEET_TEXT_BIN));
                System.out.println(record.getValue(TWEET_DATETIME_BIN));
                System.out.println();
                recordCounter++;
            }
        }
        finally {
            rs.close();
            System.out.println("TOTAL RECORDS = " + recordCounter);
            System.out.println("--------------------------------------------------------");
        }

    }

    public static void getTweetsInTimeframe(long startTime){
        System.out.println("\nGet Tweets from " + startTime + " till now" );
        aerospikeClient = new AerospikeClient(Constants.AEROSPIKE_HOST, Constants.AEROSPIKE_PORT);
        aerospikeWritePolicy = new WritePolicy();
        aerospikeWritePolicy.maxRetries = 10;
        aerospikeWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;

        Statement stmt = new Statement();
        stmt.setNamespace(AEROSPIKE_NS);
        stmt.setSetName(AEROSPIKE_STORMSET);

        long endTime = System.currentTimeMillis();
        stmt.setFilters(Filter.range(TWEET_DATETIME_BIN, startTime, endTime));

        stmt.setBinNames(TWEET_ID_BIN, USER_NAME_BIN, TWEET_DATETIME_BIN, TWEET_LOCATION_BIN, TWEET_TEXT_BIN, TWEET_HASHTAG_BIN);

        RecordSet rs = aerospikeClient.query(null, stmt);
        recordCounter = 0;

        try {
            while (rs.next()) {
                Key key = rs.getKey();
                Record record = rs.getRecord();
                System.out.println(record.getValue(USER_NAME_BIN));
                System.out.println(record.getValue(TWEET_TEXT_BIN));
                System.out.println(record.getValue(TWEET_DATETIME_BIN));
                System.out.println();
                recordCounter++;
            }
        }
        finally {
            rs.close();
            System.out.println("TOTAL RECORDS = " + recordCounter);
            System.out.println("--------------------------------------------------------");
        }

    }

     public static void main(String[] args){
         String userName = "Sofie";

         getTweetsOfUser(userName);

         int mins = 22;
         long startTime = System.currentTimeMillis() - (mins * 60 * 1000);
         getTweetsInTimeframe(startTime);

     }

}