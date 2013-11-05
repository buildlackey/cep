/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import org.testng.annotations.Test;
import storm.kafka.*;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;


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
        verifyResults(null);

    }

    @Override
    protected StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        IRichSpout feedSpout =
                new ExternalFeedToKafkaAdapterSpout(
                        new TestFeedItemProvider(),
                        BROKER_CONNECT_STRING,
                        topicName, null);
        builder.setSpout("externalFeedSpout", feedSpout);


        return builder.createTopology();
    }


    protected int getMaxAllowedToRunMillisecs() {
        return ExternalFeedToKafkaAdapterSpoutTest.MAX_ALLOWED_TO_RUN_MILLISECS;
    }
}

