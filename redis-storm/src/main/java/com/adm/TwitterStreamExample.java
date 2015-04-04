package com.adm;

import redis.clients.jedis.Jedis;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamExample {
    private class TweetsListener implements StatusListener {
        public Integer counter;

        public TweetsListener() {
            this.counter = 0;
        }

        @Override
        public void onStatus(Status status) {
            jedis.set(this.counter.toString(), status.getUser().getName() + ":" + status.getText());
            String value = jedis.get(this.counter.toString());
            System.out.println("Tweet " + this.counter + " : " + value);
            this.counter++;
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

    LinkedBlockingQueue<String> queue = null;
    Configuration configuration = null;
    Jedis jedis = null;

    public TwitterStreamExample() {
        this.queue = new LinkedBlockingQueue<String>();
        this.configuration = setupConfigurationForTwitter();
        this.jedis = new Jedis("localhost");
    }

    private Configuration setupConfigurationForTwitter() {
        ConfigurationBuilder configuration = new ConfigurationBuilder();
        configuration.setOAuthConsumerKey("===========================")
                .setOAuthConsumerSecret("===========================")
                .setOAuthAccessToken("===========================")
                .setOAuthAccessTokenSecret("===========================");
        return configuration.build();
    }

    public static void main(String[] args) throws TwitterException {
        TwitterStreamExample twitterStreamExample = new TwitterStreamExample();
        twitterStreamExample.readAndStoreTweetsInRedis();
    }

    private void readAndStoreTweetsInRedis() {
        TweetsListener listener = new TweetsListener();

        TwitterStreamFactory streamFactory = new TwitterStreamFactory(configuration);
        TwitterStream stream = streamFactory.getInstance();
        stream.addListener(listener);
        stream.sample();
    }
}
