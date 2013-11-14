/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.generated.StormTopology;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;


/**
 * This test builds on StormKafkaSpoutGetsInputViaAdaptedExternalFeedTest. Our ExternalFeedToKafkaAdapterSpout
 * pushes messages into a topic. These messages are then routed into an EsperBolt which uses the Esper query
 * language to do some simple filtering, We then route the filtered messages to a KafkaOutputBolt which
 * dumps the filtered messages on a second topic. We use an instance of Kafka MessageConsumer to pull those
 * messages off the second topic, and we verify that what we got is equal to what we expect.
 */
public class ExternalFeedRoutedToEsperAndThenToKakfaOutputBoltTest extends AbstractStormWithKafkaTest {
    public static final int EXPECTED_COUNT = 6;
    protected static volatile boolean finishedCollecting = false;

    protected static final int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 25 /* seconds */;
    protected static final int SECOND = 1000;

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
        //ServerAndThreadCoordinationUtils.pauseUntil("/tmp/before.storm");
        submitTopology();                              // The last bolt in this topology will write to second topic
        //ServerAndThreadCoordinationUtils.pauseUntil("/tmp/after.storm");
        Thread verifyThread = setupVerifyThreadToListenOnSecondTopic();
        try {
            verifyThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (!testPassed) {
            throw new RuntimeException("Test did not pass. Got messages: ");
        }
    }

    @Override
    public String getSecondTopicName() {
        return secondTopic;
    }

    @Override
    protected StormTopology createTopology() {
        return TopologyInitializer.
                createTopology(
                        getZkConnect(),
                        BROKER_CONNECT_STRING,
                        getTopicName(),
                        getSecondTopicName(),
                        new TestFeedItemProvider(getTestSentences()), false);
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
        Thread.UncaughtExceptionHandler uncaughtHandler = new Thread.UncaughtExceptionHandler() {
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
        verifyThread.setUncaughtExceptionHandler(uncaughtHandler);
        verifyThread.start();
        return verifyThread;
    }
}

