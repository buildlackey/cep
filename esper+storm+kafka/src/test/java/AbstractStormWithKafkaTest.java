/*
 * Author: cbedford
 * Date: 11/1/13
 * Time: 5:00 PM
 */


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import com.google.common.io.Files;
import kafka.admin.CreateTopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;


/**
 * Simplifies testing of Storm components that consume or produce data items from or to Kafka.
 * Operates via  a 'template method' series of steps, wherein the BeforeClass method sets up a
 * Storm Local cluster, then waits for the zookeeper instance started by that cluster to 'boot up',
 * then starts an-process Kafka server using that zookeeper, and then creates a topic whose
 * name is derived from the name of the base class test.
 * <p/>
 * Subclasses only need to implement the abstract createTopology() method (and perhaps
 * override 'verifyResults())' which is currently kind of hard coded to our first two subclasses of
 * this base class.
 */
public abstract class AbstractStormWithKafkaTest {
    public static String[] sentences = new String[]{
            "one dog9 - saw the fox over the moon",
            "two cats9 - saw the fox over the moon",
            "four bears9 - saw the fox over the moon",
            "five goats9 - saw the fox over the moon",
            "SHUTDOWN",
    };
    protected final String BROKER_CONNECT_STRING = "localhost:9092";    // kakfa broker server/port info
    private final String topicName = this.getClass().getSimpleName() + "_topic_" + getRandomInteger(1000);
    protected final String topologyName = this.getClass().getSimpleName() + "-topology" + getRandomInteger(1000);

    protected LocalCluster cluster = null;

    private final File kafkaWorkingDir = Files.createTempDir();
    private final CountDownLatch kafkaTopicCreatedLatch = new CountDownLatch(1);
    private KafkaServer kafkaServer = null;
    private Timer timer;
    private Thread kafkaServerThread = null;

    @BeforeClass(alwaysRun = true)
    protected void setUp() {
        timer = ServerAndThreadCoordinationUtils.setMaxTimeToRunTimer(getMaxAllowedToRunMillisecs());
        ServerAndThreadCoordinationUtils.removePauseSentinelFile();
        cluster = new LocalCluster();
        ServerAndThreadCoordinationUtils.waitForServerUp("localhost", 2000, 5 * KafkaOutputBoltTest.SECOND);   // Wait for zookeeper to come up

        /*
         *  Below we start up kafka and create topic in a separate thread. If we don't do this then we
         *  get very bizarre behavior, such as tuples never being emitted from our spouts and bolts
         *  as expected. Haven't figure out why this is needed... But doing it 'cause that's what makes
         *  things work.
         */
        kafkaServerThread = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        startKafkaServer();
                        createTopic(getTopicName());
                        if (getSecondTopicName() != null) {
                            createTopic(getSecondTopicName());
                        }
                        ServerAndThreadCoordinationUtils.countDown(kafkaTopicCreatedLatch);
                    }
                },
                "kafkaServerThread"
        );
        kafkaServerThread.start();
        ServerAndThreadCoordinationUtils.await(kafkaTopicCreatedLatch);
    }


    public String getSecondTopicName() {
        return null;
    }


    abstract protected int getMaxAllowedToRunMillisecs();

    @AfterClass(alwaysRun = true)
    protected void tearDown() {
        try {
            kafkaServerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.shutdown();
        kafkaServer.shutdown();
        timer.cancel();
    }

    protected void createTopic(String topicName) {
        String[] arguments = new String[6];
        arguments[0] = "--zookeeper";
        arguments[1] = "localhost:2000";
        arguments[2] = "--partition";
        arguments[3] = "1";
        arguments[4] = "--topic";
        arguments[5] = topicName;

        CreateTopicCommand.main(arguments);
    }

    protected void startKafkaServer() {
        Properties props = createProperties(kafkaWorkingDir.getAbsolutePath(), 9092, 1);
        KafkaConfig kafkaConfig = new KafkaConfig(props);

        kafkaServer = new KafkaServer(kafkaConfig, new MockTime());
        kafkaServer.startup();
    }

    protected String getZkConnect() {   // Uses zookeeper created by LocalCluster

        return "localhost:2000";
    }

    protected int getRandomInteger(int max) {
        return (int) Math.floor((Math.random() * max));
    }

    private Properties createProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("broker.id", brokerId + "");
        properties.put("log.dir", logDir);
        properties.put("zookeeper.connect", getZkConnect());
        return properties;
    }


    protected abstract StormTopology createTopology();


    /**
     * @return a Config object with time outs set very high so that the storm to zookeeper
     *         session will be kept alive, even as we are rooting around in a debugger.
     */
    public static Config getDebugConfigForStormTopology() {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 900 * 1000);
        config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 900 * 1000);
        return config;
    }

    public void verifyResults(String topic, int expectedCount) {
        if (topic == null) {
            topic = this.getTopicName();
        }
        if (expectedCount == -1) {
            expectedCount = sentences.length;
        }

        int foundCount = 0;
        KafkaMessageConsumer msgConsumer = null;
        try {
            msgConsumer = new KafkaMessageConsumer(getZkConnect(), topic);
            msgConsumer.consumeMessages();

            foundCount = 0;
            for (String msg : msgConsumer.getMessagesReceived()) {
                System.out.println("message: " + msg);
                if (msg.contains("cat") ||
                        msg.contains("dog") ||
                        msg.contains("bear") ||
                        msg.contains("goat") ||
                        msg.contains("SHUTDOWN")) {
                    foundCount++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (foundCount != expectedCount) {
            if (msgConsumer != null) {
                System.out.println("Did not receive expected messages. Got: " +
                        msgConsumer.getMessagesReceived());
            }

            throw new RuntimeException(">>>>>>>>>>>>>>>>>>>>  Did not receive expected messages");
        }
    }

    protected void submitTopology() {

        final Config conf = getDebugConfigForStormTopology();

        cluster.submitTopology(topologyName, conf, createTopology());
    }

    public String getTopicName() {
        return topicName;
    }
}
