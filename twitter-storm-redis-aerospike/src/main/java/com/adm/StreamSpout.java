package com.adm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class StreamSpout implements IRichSpout {

    private Integer counter = 0;

    // Twitter access keys
    private String consumerKey;
    private String consumerSecret;
    private String accessTokenKey;
    private String accessTokenSecret;

    // Collector for tuples
    SpoutOutputCollector spoutOutputCollector;

    // Twitter stream to read the stream
    TwitterStream twitterStream;

    // Queue to buffer incoming tuples
    LinkedBlockingQueue<Status> queue = null;

    private class TweetsListener implements StatusListener {

        @Override
        public void onStatus(Status status) {
            queue.offer(status);
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

        }

        @Override
        public void onTrackLimitationNotice(int i) {

        }

        @Override
        public void onScrubGeo(long l, long l2) {

        }

        @Override
        public void onStallWarning(StallWarning stallWarning) {

        }

        @Override
        public void onException(Exception e) {
            e.printStackTrace();
        }
    }

    public StreamSpout(String consumerKey, String consumerSecret, String accessTokenKey, String accessTokenSecret) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessTokenKey = accessTokenKey;
        this.accessTokenSecret = accessTokenSecret;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.queue = new LinkedBlockingQueue<Status>();

        ConfigurationBuilder configuration = new ConfigurationBuilder();
        configuration.setOAuthConsumerKey(this.consumerKey)
                .setOAuthConsumerSecret(this.consumerSecret)
                .setOAuthAccessToken(this.accessTokenKey)
                .setOAuthAccessTokenSecret(this.accessTokenSecret);

        TwitterStreamFactory streamFactory = new TwitterStreamFactory(configuration.setJSONStoreEnabled(true).build());
        this.twitterStream = streamFactory.getInstance();
        this.twitterStream.addListener(new TweetsListener());
        this.twitterStream.sample();
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        Status status = queue.poll();
        if (status == null) {
            Utils.sleep(1000);
            return;
        }
        this.spoutOutputCollector.emit(new Values(status));
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }
}
