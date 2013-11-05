/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import org.testng.annotations.Test;

import java.io.IOException;


public class KafkaOutputBoltTest extends AbstractStormWithKafkaTest {
    protected static final int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 10 /* seconds */;
    protected static final int SECOND = 1000;

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
        verifyResults(null);

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


    public void verifyResults(String topicName) {
        KafkaMessageConsumer msgConsumer = new KafkaMessageConsumer(getZkConnect(), this.topicName);
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

