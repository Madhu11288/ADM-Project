package adm.project.twitter_aerospike;

import java.util.Date;

import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;


public class TwitterStreamExample {
    private static final String TWEET_ID_BIN = "tweetId_bin";
    private static final String TWEET_DATE_BIN = "tweetDate";
    private static final String TWEET_LOCATION_BIN = "tweetLocation";
    private static final String TWEET_TEXT_BIN = "tweetText";
    private static final String USER_NAME_BIN = "userName";
    private static final String TWEET_ID = "tweetid";
    public static AerospikeClient client;
    public static ClientPolicy cPolicy = new ClientPolicy();
    public static WritePolicy wPolicy = new WritePolicy();
    public static Policy policy = new Policy();
    
    //private static Logger log = Logger.getLogger(TwitterStreamExample.class);

    public static void main(String[] args){
        ConfigurationBuilder configuration = getConfiguration();
        
        //aerospike key and bin
        final String namespace = "test";
        final String setName = "madhuset";
        cPolicy.timeout = 500;
        wPolicy.timeout = 50;
        client = new AerospikeClient("127.0.0.1", 3000);


        TwitterStream twitterStream = new TwitterStreamFactory(configuration.build()).getInstance();
        StatusListener listener = new StatusListener() {
            
            public void onException(Exception arg0) {
                // TODO Auto-generated method stub
                
            }
            
            public void onTrackLimitationNotice(int arg0) {
                // TODO Auto-generated method stub
                
            }
            
            public void onStatus(Status status) {
                User user = status.getUser();
                // gets Username
                String twitterUserName = user.getScreenName();
                System.out.println("Username = " +twitterUserName);
                String geoLocation = status.getGeoLocation().toString();
                System.out.println("GeoLocatio = " +geoLocation);
                long tweetId = status.getId();
                System.out.println("Tweet ID = " +tweetId);
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
                IndexTask indexTask = client.createIndex(policy, namespace, setName, 
                        TWEET_ID, TWEET_ID_BIN, IndexType.NUMERIC);
                indexTask.waitTillComplete();
                //log.info("created index");
                try{

                    Key key = new Key(namespace, setName, tweetId);
                    Bin bin1 = new Bin(USER_NAME_BIN,twitterUserName);
                    Bin bin2 = new Bin(TWEET_TEXT_BIN,content);
                    Bin bin3 = new Bin(TWEET_LOCATION_BIN,geoLocation);
                    Bin bin4 = new Bin(TWEET_DATE_BIN,createdDate.toString());
                    Bin bin5 = new Bin(TWEET_ID_BIN,tweetId);
                   client.put(wPolicy, key, bin1,bin2,bin3,bin4,bin5);

                }

                catch (AerospikeException ae){
                    ae.printStackTrace();
                    System.out.println("Aerospike Write error !!! ");
                    System.exit(-1);
                }
                

                
            }
            
            public void onStallWarning(StallWarning arg0) {
                // TODO Auto-generated method stub
                
            }
            
            public void onScrubGeo(long arg0, long arg1) {
                // TODO Auto-generated method stub
                
            }
            
            public void onDeletionNotice(StatusDeletionNotice arg0) {
                // TODO Auto-generated method stub
                
            }
        };
       

        FilterQuery fq = new FilterQuery();
        String keywords[] = {"NewYork", "Chicago"};

        fq.track(keywords);

        twitterStream.addListener(listener);
        twitterStream.filter(fq);
    }

    private static ConfigurationBuilder getConfiguration() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("0VaPFl6OeDLJh0V22JcrEqcRo");
        cb.setOAuthConsumerSecret("7YZSMsVdYUflCa9Mwumqy8mMa4iVASvV388PLHJFOA6MUfdXUm");
        cb.setOAuthAccessToken("107750863-NL1DY3q3rpAwpAvxtjDiW96lOVFVMI47AsGdCOjJ");
        cb.setOAuthAccessTokenSecret("fIIs5TTqgia5wpNks1PN4OpIzsF1dsPbCuHvHhXv1OZFR");
        return cb;
        
    }
}
