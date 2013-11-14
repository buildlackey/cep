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
 * recieves, JSON-ifies it, and dumps it on the Kafka topic that is configured in the
 * constructor.  By default the JSON-ification algorithms works such that the Json object's
 * attribute names are the field names of the tuples (currently only 1-tuples are supported).
 * In other words, the JSON-ified value is contructed as a map with key names derived from
 * tuple field names and corresponding values set as the JSON-ified tuple object.
 *
 * However, if the KafkaOutputBolt constructor is called with rawMode=true, then for a 1-tuple
 * we will assume the tuple value is a valid JSON string.   TODO - we will eventually support
 * tuples of length 2 and greater, at which point raw mode will boil down to putting the 'raw'
 * valid JSON strings given by the i-th element of each tuple into an array.
 */
public class KafkaOutputBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private final boolean rawMode;

    private String brokerConnectString;
    private String topicName;
    private String serializerClass;

    private transient Producer<String, String> producer;
    private transient OutputCollector collector;
    private transient TopologyContext context;

    public KafkaOutputBolt(String brokerConnectString,
                           String topicName,
                           String serializerClass,
                           boolean rawMode) {
        if (serializerClass == null) {
            serializerClass = "kafka.serializer.StringEncoder";
        }
        this.brokerConnectString = brokerConnectString;
        this.serializerClass = serializerClass;
        this.topicName = topicName;
        this.rawMode = rawMode;
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
        String tupleAsJson = null;
        try {
            if (rawMode) {
                tupleAsJson = input.getString(0);

            } else {
                tupleAsJson = JsonHelper.toJson(input);
            }
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
}
