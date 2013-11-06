/*
 * Author: cbedford
 * Date: 10/31/13
 * Time: 2:37 PM
 */


import com.google.common.collect.ImmutableMap;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 *  Uses Kafka high level consumer API to read from the topic passed in as a constructor argument and
 *  accumulates all messages read in so that after the test the received messages can be obtained by a
 *  call to getMessagesReceived(). This enables test driver code to verify that sent messages actually
 *  equal received messages.
 */
public class KafkaMessageConsumer {
    private final String zkConnect;

    private List<String> messagesReceived = new ArrayList<String>();
    private final String topic;
    private final String groupId = "KafkaMessageConsumer." + Math.random();

    public KafkaMessageConsumer(String zkConnect, String topic) {
        this.zkConnect = zkConnect;
        this.topic = topic;
    }

    public List<String> consumeMessages() {
        String ttt = topic;
        System.out.println("topic in kafka consumer: " + topic);
        try {
            final ConsumerConnector consumer =
                    kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
            final Map<String, Integer> topicCountMap = ImmutableMap.of(topic, 1);
            final Map<String, List<KafkaStream<String,String>>> consumerMap;

            StringDecoder decoder = new StringDecoder(new VerifiableProperties());
            consumerMap = consumer.createMessageStreams(topicCountMap, decoder,  decoder);

            final KafkaStream<String,String> stream = consumerMap.get(topic).get(0);
            final ConsumerIterator<String,String> iterator = stream.iterator();
            while (iterator.hasNext()) {
                String msg = iterator.next().message();
                msg =  ( msg == null ? "<null>" : msg );
                System.out.println("got message" + msg);
                messagesReceived.add(msg);
                if (msg.contains("SHUTDOWN")) {
                    consumer.shutdown();
                    return messagesReceived;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return messagesReceived;
    }

    public List<String> getMessagesReceived() {
        return messagesReceived;
    }


    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", groupId);
        props.put("zk.sessiontimeout.ms", "400");
        props.put("fetch.min.bytes", "1");
        props.put("auto.offset.reset", "smallest");
        props.put("zk.synctime.ms", "200");
        props.put("autocommit.interval.ms", "1000");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        return new ConsumerConfig(props);
    }

}

