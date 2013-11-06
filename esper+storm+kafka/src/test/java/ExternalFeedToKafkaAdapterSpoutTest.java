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

/**
 *  In this test messages from an external feed (a hard coded array of strings) are dumped into a
 *  Kafka topic by an instance of ExternalFeedToKafkaAdapterSpout.  We then use an instance of
 *  KafkaMessageConsumer to pull those messages off the topic, and verify that what we
 *  got is equal to what we expect.
 */
@Test
public class ExternalFeedToKafkaAdapterSpoutTest extends AbstractStormWithKafkaTest {

    protected static final int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 30 /* seconds */;
    protected static final int SECOND = 1000;


    @Test
    public void runTestWithTopology() throws IOException {
        submitTopology();
        try {
            Thread.sleep(1000 * 5);
        } catch (InterruptedException e) {
            e.printStackTrace();   // do something more meaningful here?
        }
        verifyResults(null, -1);

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


        return builder.createTopology();
    }


    protected int getMaxAllowedToRunMillisecs() {
        return ExternalFeedToKafkaAdapterSpoutTest.MAX_ALLOWED_TO_RUN_MILLISECS;
    }
}

