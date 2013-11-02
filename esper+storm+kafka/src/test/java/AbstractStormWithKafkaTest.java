/*
 * Author: cbedford
 * Date: 11/1/13
 * Time: 5:00 PM
 */


import backtype.storm.Config;
import backtype.storm.LocalCluster;
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

public abstract class AbstractStormWithKafkaTest {
    protected final String topicName =  this.getClass().getSimpleName() + "_topic_" + getRandomInteger(1000);
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
                        createTopic(topicName);
                        ServerAndThreadCoordinationUtils.countDown(kafkaTopicCreatedLatch);
                    }
                },
                "kafkaServerThread"
        );
        kafkaServerThread.start();
        ServerAndThreadCoordinationUtils.await(kafkaTopicCreatedLatch);
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

    /**
     * @return a Config object with time outs set very high so that the storm to zookeeper
     * session will be kept alive, even as we are rooting around in a debugger.
     */
    protected Config getDebugConfigForStormTopology() {
        Config  config = new Config();
        config.setDebug(true);
        config.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 600 * 1000);
        config.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 600 * 1000);
        return config;
    }
}
