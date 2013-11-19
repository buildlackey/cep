/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

import java.io.IOException;


/**
 * This example pulls tweets from twitter and runs them from a filter written in Esper query language (EQL). Our
 * ExternalFeedToKafkaAdapterSpout pushes messages into a topic. These messages are then routed into an EsperBolt which
 * uses EQL to do some simple filtering, We then route the filtered messages to a KafkaOutputBolt which
 * dumps the filtered messages on a second topic.
 */
public class EsperFilteredTwitterFeedTopology {

    private final String outputTopic = this.getClass().getSimpleName() + "_output";
    private final String firstTopic = this.getClass().getSimpleName() + "_input";

    private final String oAuthConsumerKey;
    private final String oAuthConsumerSecret;
    private final String oAuthAccessToken;
    private final String oAuthAccessTokenSecret;
    private final String brokerConnectString;                   // kakfa broker server/port info
    private final String searchTerm;                            // twitter feed filter search term


    public EsperFilteredTwitterFeedTopology(
            final String oAuthConsumerKey,
            final String oAuthConsumerSecret,
            final String oAuthAccessToken,
            final String oAuthAccessTokenSecret,
            final String brokerConnectString,
            final String searchTerm) {
        this.oAuthConsumerKey = oAuthConsumerKey;
        this.oAuthConsumerSecret = oAuthConsumerSecret;
        this.oAuthAccessToken = oAuthAccessToken;
        this.oAuthAccessTokenSecret = oAuthAccessTokenSecret;
        this.brokerConnectString = brokerConnectString;
        this.searchTerm = searchTerm;
    }

    public static void main(String[] args) throws InvalidTopologyException, AlreadyAliveException, IOException {
        if (args.length != 6) {
            throw new RuntimeException("USAGE: "
                    + "<oAuthConsumerKey> "
                    + "<oAuthConsumerSecret> "
                    + "<oAuthAccessToken> "
                    + "<oAuthAccessTokenSecret>"
                    + "<kafkaBrokerConnectString>"
                    + "<search term to use to monitor Twitter feed> "
            );
        }

        final String oAuthConsumerKey = args[0];
        final String oAuthConsumerSecret = args[1];
        final String oAuthAccessToken = args[2];
        final String oAuthAccessTokenSecret = args[3];
        final String brokerConnectString = args[4];
        final String searchTerm = args[5];


        EsperFilteredTwitterFeedTopology topology = new EsperFilteredTwitterFeedTopology(
                oAuthConsumerKey,
                oAuthConsumerSecret,
                oAuthAccessToken,
                oAuthAccessTokenSecret,
                brokerConnectString,
                searchTerm
        );
        topology.submitTopology();

    }

    public String getTopicName() {          // input topic
        return firstTopic;
    }

    public String getSecondTopicName() {   // output topic
        return outputTopic;
    }

    protected String getZkConnect() {   // Uses zookeeper created by LocalCluster
        return "localhost:2181";
    }


    public void submitTopology() throws IOException, AlreadyAliveException, InvalidTopologyException {
        System.out.println("topic: " + getTopicName() + "second topic:" + getSecondTopicName());
        final Config conf = getDebugConfigForStormTopology();
        conf.setNumWorkers(2);
        StormSubmitter.submitTopology(this.getClass().getSimpleName(), conf, createTopology());
    }

    protected StormTopology createTopology() {
        TwitterFeedItemProvider feedItemProvider = new TwitterFeedItemProvider(
                oAuthConsumerKey,
                oAuthConsumerSecret,
                oAuthAccessToken,
                oAuthAccessTokenSecret,
                searchTerm);
        return TopologyInitializer.
                createTopology(
                        getZkConnect(),
                        brokerConnectString,
                        getTopicName(),
                        getSecondTopicName(),
                        feedItemProvider,
                        true);
    }

    public static Config getDebugConfigForStormTopology() {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 900 * 1000);
        config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 900 * 1000);
        return config;
    }
}

