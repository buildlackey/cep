/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

import java.io.File;
import java.io.IOException;


/**
 * This example pulls tweets from twitter and runs them from a filter written in Esper query language (EQL). Our
 * ExternalFeedToKafkaAdapterSpout pushes messages into a topic. These messages are then routed into an EsperBolt which
 * uses EQL to do some simple filtering, We then route the filtered messages to a KafkaOutputBolt which
 * dumps the filtered messages on a second topic.
 */
public class EsperFilteredTwitterFeedTopology {
    protected final String BROKER_CONNECT_STRING = "localhost:9092";    // kakfa broker server/port info

    private final String outputTopic = this.getClass().getSimpleName() + "_output";
    private final String firstTopic  = this.getClass().getSimpleName() + "_input";
    private volatile boolean testPassed = true;   // assume the best


    public static void main(String[] args) throws InvalidTopologyException, AlreadyAliveException, IOException {
        deleteSentinelFile("/tmp/before.storm");
        deleteSentinelFile("/tmp/after.storm");

        new EsperFilteredTwitterFeedTopology().submitTopology();
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

    public static void deleteSentinelFile(String pathname) {
        File sentinel = new File(pathname);
        sentinel.delete();
        if (sentinel.exists()) {
            throw new RuntimeException("Could not delete sentinel file");
        }
    }

    public void submitTopology() throws IOException, AlreadyAliveException, InvalidTopologyException {
        System.out.println("topic: " + getTopicName() + "second topic:" + getSecondTopicName());
        final Config conf =  getDebugConfigForStormTopology();
        conf.setNumWorkers(2);
        StormSubmitter.submitTopology(this.getClass().getSimpleName(), conf, createTopology());
    }

    protected StormTopology createTopology() {
        return TopologyInitializer.
                createTopology(
                        getZkConnect(),
                        BROKER_CONNECT_STRING,
                        getTopicName(),
                        getSecondTopicName(),
                        new TwitterFeedItemProvider("#reviews"),
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

