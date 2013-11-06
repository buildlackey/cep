/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import org.testng.annotations.Test;
import storm.kafka.*;

import java.io.IOException;


/**
 * This test builds on StormKafkaSpoutGetsInputViaAdaptedExternalFeedTest. Our ExternalFeedToKafkaAdapterSpout
 * pushes messages into a topic. These messages are then routed into an EsperBolt which uses the Esper query
 * language to do some simple filtering, We then route the filtered messages to a KafkaOutputBolt which
 * dumps the filtered messages on a second topic. We use an instance of Kafka MessageConsumer to pull those
 * messages off the second topic, and we verify that what we got is equal to what we expect.
 */
@Test
public class ExternalFeedRoutedToEsperAndThenToKakfaOutputBoltTest extends AbstractStormWithKafkaTest {
    protected static volatile boolean finishedCollecting = false;

    protected static final int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 2000 /* seconds */;
    protected static final int SECOND = 1000;

    private static int STORM_KAFKA_FROM_READ_FROM_START = -2;
    private static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
    private final String secondTopic = this.getClass().getSimpleName() + "topic" + getRandomInteger(1000);


    @Test
    public void runTestWithTopology() throws IOException {
        System.out.println("topic: " + getTopicName() + "second topic:" + getSecondTopicName());
        Thread verifyThread = setupVerifyThreadToListenOnSecondTopic();
        submitTopology();  // The last bolt in this topology will write to second topic
        try {
            verifyThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("}}}}ENDING");
    }

    @Override
    public String getSecondTopicName() {
        return secondTopic;
    }

    @Override
    protected StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        IRichSpout feedSpout =
                new ExternalFeedToKafkaAdapterSpout(
                        new TestFeedItemProvider(getTestSentences()),
                        BROKER_CONNECT_STRING,
                        getTopicName(), null);
        builder.setSpout("externalFeedSpout", feedSpout);
        builder.setSpout("kafkaSpout", createKafkaSpout());

        KafkaOutputBolt kafkaOutputBolt =
                new KafkaOutputBolt(BROKER_CONNECT_STRING, getSecondTopicName(), null);
        builder.setBolt("kafkaOutputBolt", kafkaOutputBolt, 1)
                .shuffleGrouping("kafkaSpout");

        return builder.createTopology();
    }

    protected int getMaxAllowedToRunMillisecs() {
        return ExternalFeedRoutedToEsperAndThenToKakfaOutputBoltTest.MAX_ALLOWED_TO_RUN_MILLISECS;
    }

    private void waitForResultsFromStormKafkaSpoutToAppearInCollectorBolt() {
        while (!finishedCollecting) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("DONE");
    }

    private KafkaSpout createKafkaSpout() {
        BrokerHosts brokerHosts = new ZkHosts(getZkConnect());
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, getTopicName(), "", "storm");
        kafkaConfig.forceStartOffsetTime(STORM_KAFKA_FROM_READ_FROM_START);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(kafkaConfig);
    }


    private String[] getTestSentences() {
        return new String[]{
                "one dog9 - saw the fox over the moon",
                "two cats9 - saw the fox over the moon",
                "four bears9 - saw the fox over the moon",
                "five goats9 - saw the fox over the moon",
                "SHUTDOWN",
        };

    }

    private Thread setupVerifyThreadToListenOnSecondTopic() {
        Thread verifyThread = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        verifyResults(getSecondTopicName());
                    }
                },
                "verifyThread"
        );
        verifyThread.start();
        return verifyThread;
    }
}

