/*
 * Author: cbedford
 * Date: 10/30/13
 * Time: 9:39 PM
 */


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A first pass implementation of a generic Kafka Output Bolt that takes whatever tuple it
 * recieves, JSON-ifies it, and dumps it on the Kafka topic that is configured int the
 * constructor.
 */
public class KafkaOutputBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private String brokerConnectString;
    private String topicName;
    private String serializerClass;

    private transient Producer<String, String> producer;
    private transient OutputCollector collector;
    private transient TopologyContext context;

    public KafkaOutputBolt(String brokerConnectString,
                           String topicName,
                           String serializerClass) {
        if (serializerClass == null) {
            serializerClass = "kafka.serializer.StringEncoder";
        }
        this.brokerConnectString = brokerConnectString;
        this.serializerClass = serializerClass;
        this.topicName = topicName;
    }

    @Override
    public void prepare(Map stormConf,
                        TopologyContext context,
                        OutputCollector collector) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerConnectString);
        props.put("serializer.class", serializerClass);
        props.put("producer.type", "sync");
        props.put("batch.size", "1");

        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);

        this.context = context;
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String tupleAsJson = JsonHelper.toJson(input);
            KeyedMessage<String, String> data =
                    new KeyedMessage<String, String>(topicName, tupleAsJson);
            producer.send(data);
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public static Producer<String, String> initProducer() throws IOException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        props.put("batch.size", "1");
        ProducerConfig config = new ProducerConfig(props);

        return new Producer<String, String>(config);
    }

    public static void sendMessage(String topic, String msg, Producer<String, String> producer) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
        producer.send(data);
    }
}
