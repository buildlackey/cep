/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class TestTopology {


    final static int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 90 /* seconds */;

    CountDownLatch topologyStartedLatch = new CountDownLatch(1);

    private static int STORM_KAFKA_FROM_READ_FROM_START = -2;
    private static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
    private static int readFromMode = STORM_KAFKA_FROM_READ_FROM_START;
    private int expectedNumMessages = 8;

    private static final int SECOND = 1000;
    private static List<String> messagesReceived = new ArrayList<String>();

    private LocalCluster cluster = new LocalCluster();

    private static final String TOPIC_NAME = "big-topix-" + new Random().nextInt();
    volatile static boolean finishedCollecting = false;

    private static String[] sentences = new String[]{
            "one dog9 - saw the fox over the moon",
            "two cats9 - saw the fox over the moon",
            "four bears9 - saw the fox over the moon",
            "five goats9 - saw the fox over the moon",
    };

    private KafkaProducer kafkaProducer = new  KafkaProducer(sentences, TOPIC_NAME, topologyStartedLatch);


    public static void recordRecievedMessage(String msg) {
        synchronized (TestTopology.class) {                 // ensure visibility of list updates between threads
            messagesReceived.add(msg);
        }
    }


    public static void main(String[] args) {
        TestTopology testTopology = new TestTopology();

        if (args.length == 1 && args[0].equals("--fromCurrent")) {
            readFromMode = STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET;
            testTopology.expectedNumMessages  = 4;
        }

        testTopology.runTest();
    }

    private void runTest() {
        ServerAndThreadCoordinationUtils.setMaxTimeToRunTimer(MAX_ALLOWED_TO_RUN_MILLISECS);
        ServerAndThreadCoordinationUtils.waitForServerUp("localhost", 2000, 5 * SECOND);   // Wait for zookeeper to come up

        kafkaProducer.startKafkaServer();
        kafkaProducer.createTopic(TOPIC_NAME);

        try {


            kafkaProducer.startProducer();
            ServerAndThreadCoordinationUtils.await(kafkaProducer.producerFinishedInitialBatchLatch);

            setupKafkaSpoutAndSubmitTopology();
            try {
                Thread.sleep(5000);                 // Would be nice to have a call back inform us when ready
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ServerAndThreadCoordinationUtils.countDown(topologyStartedLatch);

            awaitResults();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        verifyResults();
        shutdown();
        System.out.println("SUCCESSFUL COMPLETION");
        System.exit(0);
    }



    private void awaitResults() {
        while (!finishedCollecting) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Sleep another couple of seconds in case any more messages than expected come into the bolt.
        // In this case the bolt should throw a fatal error
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        System.out.println("after await");
    }

    private void verifyResults() {
        synchronized (TestTopology.class) {                 // ensure visibility of list updates between threads
            int count = 0;
            for (String msg : messagesReceived) {
                if (msg.contains("cat") || msg.contains("dog") || msg.contains("bear") || msg.contains("goat")) {
                    count++;
                }
            }
            if (count != expectedNumMessages) {
                System.out.println(">>>>>>>>>>>>>>>>>>>>FAILURE -   Did not receive expected messages");
                System.exit(-1);
            }

        }
    }

    private void setupKafkaSpoutAndSubmitTopology() throws InterruptedException {
        BrokerHosts brokerHosts = new ZkHosts("localhost:2000");

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
        kafkaConfig.forceStartOffsetTime(readFromMode  /* either earliest or current offset */);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new KafkaSpout(kafkaConfig), 1);
        VerboseCollectorBolt bolt = new VerboseCollectorBolt(expectedNumMessages);
        builder.setBolt("print", bolt).shuffleGrouping("words");


        Config config = new Config();

        cluster.submitTopology("kafka-test", config, builder.createTopology());
    }

    private void shutdown() {
        cluster.shutdown();
        kafkaProducer.shutdown();
    }



}
