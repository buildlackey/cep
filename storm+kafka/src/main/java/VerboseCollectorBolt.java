import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.concurrent.CountDownLatch;

public class VerboseCollectorBolt extends BaseBasicBolt {

    ;
    private int expectedNumMessages;
    private int countReceivedMessages = 0;

    VerboseCollectorBolt(int expectedNumMessages) {
        this.expectedNumMessages = expectedNumMessages;
    }



    public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        final String msg = tuple.toString();

        countReceivedMessages++;
        String info = " recvd: " + countReceivedMessages + " expected: " + expectedNumMessages;
        System.out.println(info +    " >>>>>>>>>>>>>" + msg);

        TestTopology.recordRecievedMessage(msg);
        if (countReceivedMessages == expectedNumMessages) {
            System.out.println(" +++++++++++++++++++++ MARKING");
            TestTopology.finishedCollecting = true;
        }

        if (countReceivedMessages > expectedNumMessages) {
            System.out.print("Fatal error: too many messages received");
            System.exit(-1);
        }
    }
}
