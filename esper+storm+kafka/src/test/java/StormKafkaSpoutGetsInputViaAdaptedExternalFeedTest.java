/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import org.testng.annotations.Test;
import storm.kafka.*;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;


@Test
public class StormKafkaSpoutGetsInputViaAdaptedExternalFeedTest extends AbstractStormWithKafkaTest {
    protected static volatile boolean finishedCollecting = false;

    protected static final int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 20 /* seconds */;
    protected static final int SECOND = 1000;

    private static int STORM_KAFKA_FROM_READ_FROM_START = -2;
    private static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;


    @Test
    public void runTestWithTopology() throws IOException {
        System.out.println("topic: " + topicName);
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
                        topicName, null);
        builder.setSpout("externalFeedSpout", feedSpout);
        builder.setSpout("kafkaSpout", createKafkaSpout());
        VerboseCollectorBolt bolt = new VerboseCollectorBolt(5);
        builder.setBolt("collector", bolt).shuffleGrouping("kafkaSpout");

        return builder.createTopology();
    }


    private   KafkaSpout createKafkaSpout() {
        BrokerHosts brokerHosts = new ZkHosts(getZkConnect());
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topicName, "", "storm");
        kafkaConfig.forceStartOffsetTime(STORM_KAFKA_FROM_READ_FROM_START);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(kafkaConfig);
    }


    protected int getMaxAllowedToRunMillisecs() {
        return StormKafkaSpoutGetsInputViaAdaptedExternalFeedTest.MAX_ALLOWED_TO_RUN_MILLISECS;
    }


    /*
        use this in to be written test...
    @Override
    public String getSecondTopicName() {
        return this.getClass().getSimpleName() + "topic" + getRandomInteger(1000);
    }
     */
}

