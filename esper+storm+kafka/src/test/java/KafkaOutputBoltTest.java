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
        verifyResults(null, -1);

    }


    protected StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        IRichSpout spout = new SentenceSpout(sentences);
        KafkaOutputBolt kafkaOutputBolt =
                new KafkaOutputBolt(BROKER_CONNECT_STRING, getTopicName(), null, false);

        builder.setSpout("sentenceSpout", spout);
        builder.setBolt("kafkaOutputBolt", kafkaOutputBolt, 1)
                .shuffleGrouping("sentenceSpout");

        return builder.createTopology();
    }


    protected int getMaxAllowedToRunMillisecs()  {
        return KafkaOutputBoltTest.MAX_ALLOWED_TO_RUN_MILLISECS;
    }
}

