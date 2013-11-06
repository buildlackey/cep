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
 * This test builds on ExternalFeedToKafkaAdapterSpoutTest. The external feed messages are dumped
 * into a Kafka topic by ExternalFeedToKafkaAdapterSpout as in the first test. We add the second step
 * of pulling the messages from the topic by a KafkaSpout and making sure those messages are what
 * we expect.  To clarify:  ExternalFeedToKafkaAdapterSpout pushes messages into a topic, and KafkaSpout
 * pulls messages out of a topic.
 */
@Test
public class StormKafkaSpoutGetsInputViaAdaptedExternalFeedTest extends AbstractStormWithKafkaTest {
    protected static volatile boolean finishedCollecting = false;

    protected static final int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 20 /* seconds */;
    protected static final int SECOND = 1000;

    private static int STORM_KAFKA_FROM_READ_FROM_START = -2;
    private static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;


    @Test
    public void runTestWithTopology() throws IOException {
        System.out.println("topic: " + getTopicName());
        submitTopology();
        waitForResultsFromStormKafkaSpoutToAppearInCollectorBolt();
        verifyResults(null);

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

    @Override
    protected StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        IRichSpout feedSpout =
                new ExternalFeedToKafkaAdapterSpout(
                        new TestFeedItemProvider(),
                        BROKER_CONNECT_STRING,
                        getTopicName(), null);
        builder.setSpout("externalFeedSpout", feedSpout);
        builder.setSpout("kafkaSpout", createKafkaSpout());
        VerboseCollectorBolt bolt = new VerboseCollectorBolt(5);
        builder.setBolt("collector", bolt).shuffleGrouping("kafkaSpout");

        return builder.createTopology();
    }


    private   KafkaSpout createKafkaSpout() {
        BrokerHosts brokerHosts = new ZkHosts(getZkConnect());
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, getTopicName(), "", "storm");
        kafkaConfig.forceStartOffsetTime(STORM_KAFKA_FROM_READ_FROM_START);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(kafkaConfig);
    }


    protected int getMaxAllowedToRunMillisecs() {
        return StormKafkaSpoutGetsInputViaAdaptedExternalFeedTest.MAX_ALLOWED_TO_RUN_MILLISECS;
    }
}

