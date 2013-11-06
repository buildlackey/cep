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


@Test
public class ExternalFeedRoutedToEsperAndThenToKakfaOutputBoltTest extends AbstractStormWithKafkaTest {
    protected static volatile boolean finishedCollecting = false;

    protected static final int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 20 /* seconds */;
    protected static final int SECOND = 1000;

    private static int STORM_KAFKA_FROM_READ_FROM_START = -2;
    private static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
    private final String secondTopic =  this.getClass().getSimpleName() + "topic" + getRandomInteger(1000);


    @Test
    public void runTestWithTopology() throws IOException {
        System.out.println("topic: " + getTopicName() + "second topic:" + getSecondTopicName());
        Thread verifyThread =  setupVerifyThreadToListenOnSecondTopic();
        submitTopology();  // The last bolt in this topology will write to second topic
        try {
            verifyThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("}}}}ENDING");
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

    @Override
    public String getSecondTopicName() {
        return  secondTopic;
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

        KafkaOutputBolt kafkaOutputBolt =
                new KafkaOutputBolt(BROKER_CONNECT_STRING, getSecondTopicName(), null);
        builder.setBolt("kafkaOutputBolt", kafkaOutputBolt, 1)
                .shuffleGrouping("kafkaSpout");

        return builder.createTopology();
    }


    private KafkaSpout createKafkaSpout() {
        BrokerHosts brokerHosts = new ZkHosts(getZkConnect());
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, getTopicName(), "", "storm");
        kafkaConfig.forceStartOffsetTime(STORM_KAFKA_FROM_READ_FROM_START);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(kafkaConfig);
    }


    protected int getMaxAllowedToRunMillisecs() {
        return ExternalFeedRoutedToEsperAndThenToKakfaOutputBoltTest.MAX_ALLOWED_TO_RUN_MILLISECS;
    }

}

