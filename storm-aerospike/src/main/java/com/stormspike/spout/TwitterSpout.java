package com.stormspike.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


public class TwitterSpout extends BaseRichSpout {

	SpoutOutputCollector collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
	String[] keyWords;

	public TwitterSpout(String consumerKey, String consumerSecret,
                        String accessToken, String accessTokenSecret, String[] keyWords) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
		this.keyWords = keyWords;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		StatusListener listener = new StatusListener() {

			public void onStatus(Status status) {

				queue.offer(status);
			}

			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			public void onTrackLimitationNotice(int i) {
			}

			public void onScrubGeo(long l, long l1) {
			}

			public void onException(Exception ex) {
			}

			public void onStallWarning(StallWarning arg0) {

			}

		};

		TwitterStream twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		twitterStream.setOAuthAccessToken(token);

		if (keyWords.length == 0) {

			twitterStream.sample();
		}

		else {

			FilterQuery query = new FilterQuery().track(keyWords);
			twitterStream.filter(query);
		}

	}

	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(ret));

		}
	}

	public void close() {
		this.twitterStream.shutdown();
	}


	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	public void ack(Object id) {
	}


	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
