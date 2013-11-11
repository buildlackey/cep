/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.tomdz.storm.esper.EsperBolt;
import storm.kafka.*;

import java.io.File;
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
    public static final int EXPECTED_COUNT = 6;
    protected static volatile boolean finishedCollecting = false;

    protected static final int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 15 /* seconds */;
    protected static final int SECOND = 1000;

    private static int STORM_KAFKA_FROM_READ_FROM_START = -2;
    private static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
    private final String secondTopic = this.getClass().getSimpleName() + "topic" + getRandomInteger(1000);
    private volatile boolean testPassed = true;   // assume the best

    @BeforeClass
    protected void deleteFiles() {
        deleteSentinelFile("/tmp/before.storm");
        deleteSentinelFile("/tmp/after.storm");
    }

    private void deleteSentinelFile(String pathname) {
        File sentinel = new File(pathname);
        sentinel.delete();
        if (sentinel.exists()) {
            throw new RuntimeException("Could not delete sentinel file");
        }
    }

    @Test
    public void runTestWithTopology() throws IOException {
        System.out.println("topic: " + getTopicName() + "second topic:" + getSecondTopicName());
        ServerAndThreadCoordinationUtils.pauseUntil("/tmp/before.storm");
        submitTopology();                              // The last bolt in this topology will write to second topic
        ServerAndThreadCoordinationUtils.pauseUntil("/tmp/after.storm");
        Thread verifyThread = setupVerifyThreadToListenOnSecondTopic();
        try {
            verifyThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (! testPassed) {
            throw new RuntimeException("Test did not pass. Got messages: " );
        }
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
        EsperBolt esperBolt = createEsperBolt();
        KafkaOutputBolt kafkaOutputBolt =
                new KafkaOutputBolt(BROKER_CONNECT_STRING, getSecondTopicName(), null);

        builder.setSpout("externalFeedSpout", feedSpout);   // these spouts are bound together by shared topic
        builder.setSpout("kafkaSpout", createKafkaSpout());

        builder.setBolt("esperBolt", esperBolt, 1)
                .shuffleGrouping("kafkaSpout");
        builder.setBolt("kafkaOutputBolt", kafkaOutputBolt, 1)
                .shuffleGrouping("esperBolt");

        return builder.createTopology();
    }

    protected int getMaxAllowedToRunMillisecs() {
        return ExternalFeedRoutedToEsperAndThenToKakfaOutputBoltTest.MAX_ALLOWED_TO_RUN_MILLISECS;
    }

    private EsperBolt createEsperBolt() {
        String esperQuery=
                "select  str as found from OneWordMsg.win:length_batch(2) where str like '%at%'";
        EsperBolt esperBolt = new EsperBolt.Builder()
                .inputs().aliasComponent("kafkaSpout").
                        withFields("str").ofType(String.class).toEventType("OneWordMsg")
                .outputs().onDefaultStream().emit("found")
                .statements().add(esperQuery)
                .build();
        return esperBolt;
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


    // EXPECTED_COUNT - consumer will see 6 occurrences of cat out of 6 batches of 2
    // The shutdown will trigger when we see the first 'cat - SHUTDOWN'. That's why the
    // consumer does not see 7 cats.
    private String[] getTestSentences() {
        return new String[]{
                "cat",
                "pig",

                "pig",
                "pig",

                "pig",
                "cat",

                "cat",
                "pig",

                "cat",
                "cat",

                "cat - SHUTDOWN",
                "cat - SHUTDOWN",
        };

    }

    private Thread setupVerifyThreadToListenOnSecondTopic() {
        Thread.UncaughtExceptionHandler uncaughtHandler  = new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread th, Throwable ex) {
                testPassed = false;
            }
        };
        Thread verifyThread = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        verifyResults(getSecondTopicName(), EXPECTED_COUNT);
                    }
                },
                "verifyThread"
        );
        verifyThread.setUncaughtExceptionHandler( uncaughtHandler );
        verifyThread.start();
        return verifyThread;
    }
}

