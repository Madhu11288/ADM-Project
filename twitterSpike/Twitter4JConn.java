package twitterconn;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Created by tvsamartha on 3/30/15.
 */



public class Twitter4JConn {

    private static Logger LOG = Logger.getLogger(Twitter4JConn.class);

    public static void main(String[] args) {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("0VaPFl6OeDLJh0V22JcrEqcRo")
                .setOAuthConsumerSecret("7YZSMsVdYUflCa9Mwumqy8mMa4iVASvV388PLHJFOA6MUfdXUm")
                .setOAuthAccessToken("107750863-NL1DY3q3rpAwpAvxtjDiW96lOVFVMI47AsGdCOjJ")
                .setOAuthAccessTokenSecret("fIIs5TTqgia5wpNks1PN4OpIzsF1dsPbCuHvHhXv1OZFR");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        try {
            Query query = new Query("@sharz0707");
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText().toString());
                }
            } while ((query = result.nextQuery()) != null);
            System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }
    }

}
