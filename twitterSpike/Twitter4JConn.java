package twitterconn;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.RegisterTask;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.util.List;

/**
 * Created by tvsamartha on 3/30/15.
 */



public class Twitter4JConn {

    private static Logger LOG = Logger.getLogger(Twitter4JConn.class);

    int port = 3000;
    String seedHost = "127.0.0.1";
    static AerospikeClient client;
    static ClientPolicy cPolicy = new ClientPolicy();
    static WritePolicy wPolicy = new WritePolicy();



    public static void main(String[] args) {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("0VaPFl6OeDLJh0V22JcrEqcRo")
                .setOAuthConsumerSecret("7YZSMsVdYUflCa9Mwumqy8mMa4iVASvV388PLHJFOA6MUfdXUm")
                .setOAuthAccessToken("107750863-NL1DY3q3rpAwpAvxtjDiW96lOVFVMI47AsGdCOjJ")
                .setOAuthAccessTokenSecret("fIIs5TTqgia5wpNks1PN4OpIzsF1dsPbCuHvHhXv1OZFR");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        int port = 3000;
        String seedHost = "127.0.0.1";

        cPolicy.timeout = 500;
        wPolicy.timeout = 50;
        client = new AerospikeClient(cPolicy, seedHost, port);


        //aerospike key and bin
        String ASPnameSp = "test";
        String ASPset = "myset";
        String ASPbin = "mybin";
        Key k = new Key(ASPnameSp, ASPset, "");
        Bin bin = new Bin(ASPbin, "");


        String twitterUserName = new String();
        String tweetText = new String();

        try {
            Query query = new Query("@aerospikedb");
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    twitterUserName = "@" + tweet.getUser().getScreenName();
                    tweetText = tweet.getText().toString();

                    System.out.println(twitterUserName + " -- " + tweetText);


                }
            } while ((query = result.nextQuery()) != null);
            //System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }

        try {
            System.out.println("Aerospike Read check ... ");
            k = new Key(ASPnameSp, ASPset, "@PeterCorless");
            Record record = client.get(wPolicy, k);
            System.out.println(record.bins.size());
            System.out.println(record.bins.values());
            // REGISTER module 'udf/profile.lua'
//            File udfFile = new File("udf/profile.lua");
//            RegisterTask task = client.register(null,
//                    udfFile.getPath(),
//                    udfFile.getName(),
//                    Language.LUA);
//            task.waitTillComplete();
//
//            System.out.println("register udf/profile.lua");
//
//            Statement stmt = new Statement();
//            stmt.setNamespace("test");
//            stmt.setSetName("profile");
//            stmt.setFilters(Filter.contains("username", IndexCollectionType.LIST, String.valueOf(Value.get("db"))));
//            ResultSet resultSet = client.queryAggregate(null, stmt,
//                    "profile", "check_password" , Value.get("ghjks"));
//
//
//
//            int count = 0;
//            try {
//
//
//                while (resultSet.next()) {
//                    Object object = resultSet.getObject();
//                    System.out.println("Result: " + object);
//                    count++;
//                }
//
//                if (count == 0) {
//                    System.out.println("No results returned.");
//                }
//            }
//            finally {
//                resultSet.close();
//                System.out.println("Count = " + count);
//            }

        } catch (AerospikeException ae){
            ae.printStackTrace();
            System.out.println("Aerospike REad error !!! ");
            System.exit(-1);
        }

        client.close();

    }

}
