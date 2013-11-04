/*
 * Author: cbedford
 * Date: 11/2/13
 * Time: 5:58 PM
 */


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.testng.annotations.*;
import org.testng.TestNG;
import org.testng.TestListenerAdapter;

import static org.easymock.EasyMock.*;

import org.slf4j.*;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class VerifyItemsFromFeedAreSentToMockKafkaProducer {
    private static String TOPIC = "someTopic";
    private static String MESSAGE = "i-am-a-message";

    private static class TestItemProvider implements  IFeedItemProvider {
        ConcurrentLinkedQueue<String> itemQueue = new ConcurrentLinkedQueue<String>();

        @Override
        public Runnable getRunnableTask() {
            return new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    itemQueue.offer(MESSAGE);
                }
            };
        }

        @Override
        public Object getNextItemIfAvailable() {
            return itemQueue.poll();
        }
    }


    @Test(enabled = true)
    public void  testEqualsOfKeyedMessage() {
        KeyedMessage<String, String>
                data1 =
                new KeyedMessage<String, String>("foo", "bar");
        KeyedMessage<String, String>
                data2 =
                new KeyedMessage<String, String>(
                        new String("foo".getBytes()),   new String("bar".getBytes()));

        assert data1.equals(data2);
    }


    @Test(enabled = true)
    public void testItemsProducedByFeedProviderAreSentToKafka() {
        Capture<KeyedMessage<String, String>>   capturedArgument =
                new Capture<KeyedMessage<String, String>> ();


        @SuppressWarnings("unchecked")
        Producer<String, String> producer =  createMock(Producer.class);
        producer.send(capture(capturedArgument));
        expectLastCall();

        ExternalFeedToKafkaAdapterSpout spout =
                EasyMock.createMockBuilder(ExternalFeedToKafkaAdapterSpout.class).
                        addMockedMethod("setupProducer").createMock();
        expect(spout.setupProducer()).andReturn(producer);


        replay(producer);
        replay(spout);



        verifyNextTupleReceivesItemFromProviderAndSendsToKafkaProducer(spout);

        verify(producer);
        verify(spout);

        KeyedMessage<String, String> got =  capturedArgument.getValue();
        assert  got.message().contains(MESSAGE);

    }

    private void verifyNextTupleReceivesItemFromProviderAndSendsToKafkaProducer(
            ExternalFeedToKafkaAdapterSpout spout)
    {
        spout.setFeedProvider(new TestItemProvider());
        spout.setTopicName(TOPIC);
        spout.open(null, null, null);

        for (int i = 0; i < 10; i++) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();   // do something more meaningful here?
            }
            spout.nextTuple();
        }
    }

}





