/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import com.google.common.io.Files;
import kafka.admin.CreateTopicCommand;
import kafka.javaapi.producer.Producer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;


public class KafkaOutputBoltTest extends AbstractStormWithKafkaTest {
    protected static final int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 30 /* seconds */;
    protected static final int SECOND = 1000;
    public static final String BROKER_CONNECT_STRING = "localhost:9092";    // kakfa broker server/port info

    private final String topologyName = this.getClass().getSimpleName() + "-topology" + getRandomInteger(1000);

    private static String[] sentences = new String[]{
            "one dog9 - saw the fox over the moon",
            "two cats9 - saw the fox over the moon",
            "four bears9 - saw the fox over the moon",
            "five goats9 - saw the fox over the moon",
            "SHUTDOWN",
    };


    @Test
    public void runTestWithTopology() throws IOException {
        submitTopology();
        verifyResults();

    }


    protected void submitTopology() {

        final Config   conf = getDebugConfigForStormTopology();

        cluster.submitTopology(topologyName, conf, createTopology());
    }

    protected StormTopology createTopology() {
        IRichSpout spout = new SentenceSpout(sentences);

        KafkaOutputBolt kafkaOutputBolt = new KafkaOutputBolt(BROKER_CONNECT_STRING, topicName, null);


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("sentenceSpout", spout);
        builder.setBolt("kafkaOutputBolt", kafkaOutputBolt, 1)
                .shuffleGrouping("sentenceSpout");

        return builder.createTopology();
    }


    public void verifyResults() {
        KafkaMessageConsumer msgConsumer = new KafkaMessageConsumer(getZkConnect(), topicName);
        msgConsumer.consumeMessages();

        int foundCount = 0;
        for (String msg : msgConsumer.getMessagesReceived()) {
            System.out.println("message: "+msg);
            if (msg.contains("cat") ||
                    msg.contains("dog") ||
                    msg.contains("bear") ||
                    msg.contains("goat") ||
                    msg.contains("SHUTDOWN")) {
                foundCount++;
            }
        }

        if (foundCount != sentences.length) {
            throw new RuntimeException(">>>>>>>>>>>>>>>>>>>>  Did not receive expected messages");
        }
    }


    protected int getMaxAllowedToRunMillisecs()  {
        return KafkaOutputBoltTest.MAX_ALLOWED_TO_RUN_MILLISECS;
    }
}

