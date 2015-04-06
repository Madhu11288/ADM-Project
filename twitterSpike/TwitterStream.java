package twitterconn;
;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Date;


/**
 * Created by tvsamartha on 4/2/15.
 */
public class TwitterStream {

    static AerospikeClient client;
    static ClientPolicy cPolicy = new ClientPolicy();
    static WritePolicy wPolicy = new WritePolicy();

    public static void main(String[] args) {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("0VaPFl6OeDLJh0V22JcrEqcRo");
        cb.setOAuthConsumerSecret("7YZSMsVdYUflCa9Mwumqy8mMa4iVASvV388PLHJFOA6MUfdXUm");
        cb.setOAuthAccessToken("107750863-NL1DY3q3rpAwpAvxtjDiW96lOVFVMI47AsGdCOjJ");
        cb.setOAuthAccessTokenSecret("fIIs5TTqgia5wpNks1PN4OpIzsF1dsPbCuHvHhXv1OZFR");

        //aerospike key and bin
        final String ASPnameSp = "test";
        final String ASPset = "myset";
        final String ASPbin = "mybin";
        cPolicy.timeout = 500;
        wPolicy.timeout = 50;
        client = new AerospikeClient(cPolicy, "127.0.0.1", 3000);


        twitter4j.TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        StatusListener listener = new StatusListener() {

            @Override
            public void onException(Exception arg0) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onScrubGeo(long arg0, long arg1) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onStatus(Status status) {
                User user = status.getUser();

                // gets Username
                String twitterUserName = status.getUser().getScreenName();
                System.out.println( twitterUserName);
                String profileLocation = user.getLocation();
                System.out.println( profileLocation);
                String geoLocation = status.getGeoLocation().toString();
                System.out.println( geoLocation);
                //long tweetId = status.getId();
                //System.out.println(tweetId);
                String content = status.getText();

                if(content.length() > 0)
                    System.out.println("Tweet = " + content + "\n");
                else
                    System.out.println("No twwet text");
                Date createdDate =  status.getCreatedAt();
                System.out.println("Created date = " + createdDate);
                int retweetCount = status.getRetweetCount();
                System.out.println("Retweet count = " + retweetCount);
                HashtagEntity[] htEntity = status.getHashtagEntities();
                for (HashtagEntity ht : htEntity){

                    System.out.println("hashtag entities = " + ht.getText());
                }
                System.out.println("-------------------------------------------------");

                /*try{

                    Key k = new Key(ASPnameSp, ASPset, twitterUserName);
                    Bin bin = new Bin(ASPbin, content);
                    client.put(wPolicy, k, bin);

                }

                catch (AerospikeException ae){
                    ae.printStackTrace();
                    System.out.println("Aerospike Write error !!! ");
                    System.exit(-1);
                }
                */

            }

            public void onTrackLimitationNotice(int arg0) {
                // TODO Auto-generated method stub

            }

        };

        FilterQuery fq = new FilterQuery();
        String keywords[] = {"NewYork", "Chicago"};

        fq.track(keywords);

        twitterStream.addListener(listener);
        twitterStream.filter(fq);

    }


}
