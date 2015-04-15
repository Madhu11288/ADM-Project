package topology;


public class TwitterTopology {
    public static void main(String[] args) {
        String consumerKey = "2mENRlpi5aGK7FRzKnl0c2h1X";
        String consumerSecret = "Kq1J74XDInqMl4MFQwGutxDR92MtNKq9Qc7aKfyZN8JCNcnTuI";
        String accessToken = "850496430-YMenq9JAlax4fgeodkVTtVAafa49GXT0evpe3aQZ";
        String accessTokenSecret = "K0eXyBqGEoh2pJ1sRqB2TcCRSYcaJBUYTVKRq0rhcWofP";
        String[] arguments = {consumerKey,consumerSecret,accessToken,accessTokenSecret};
        String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret,
                accessToken, accessTokenSecret, keyWords));
        builder.setBolt("print", new AerospikeBolt("127.0.0.1",3000,"test","madhustorm"))
                .shuffleGrouping("twitter");


        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

        Utils.sleep(50000);
        cluster.shutdown();
    }
}
