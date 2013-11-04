import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;

/**
 * A
 */
public class ExternalFeedToKafkaAdapterSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    public static final String RECORD = "record";


    private String brokerConnectString;

    private String topicName;
    private String serializerClass;

    private transient SpoutOutputCollector collector;
    private transient TopologyContext context;
    private transient Producer<String, String> producer;

    private IFeedItemProvider feedProvider;

    public ExternalFeedToKafkaAdapterSpout(IFeedItemProvider feedProvider,
                                           String brokerConnectString,
                                           String topicName,
                                           String serializerClass) {
        this.feedProvider = feedProvider;
        this.brokerConnectString = brokerConnectString;
        this.topicName = topicName;
        if (serializerClass == null) {
            serializerClass = "kafka.serializer.StringEncoder";
        }
        this.serializerClass = serializerClass;
    }


    public void setFeedProvider(IFeedItemProvider feedProvider) { // mainly for testing
        this.feedProvider = feedProvider;
    }

    public void setTopicName(String topicName) { // mainly for testing
        this.topicName = topicName;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RECORD));
    }


    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf,
                     TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;

        producer = setupProducer();

        Thread feedProviderThread =
                new Thread(feedProvider.getRunnableTask(), "feedProviderThread");
        feedProviderThread.start();
    }


    @Override
    public void nextTuple() {
        try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
        }

        Object feedItem = feedProvider.getNextItemIfAvailable();

        if (feedItem != null) {
            System.out.println(">>>->>feed item is: " + feedItem);
            final Map<String, Object> itemAsMap = ImmutableMap.of(RECORD, feedItem);
            try {
                String itemAsJson = new Gson().toJson(itemAsMap);
                KeyedMessage<String, String> data =
                        new KeyedMessage<String, String>(topicName, itemAsJson);
                producer.send(data);
            } catch (Exception e) {
                throw new RuntimeException("Conversion to json failed: " + feedItem);
            }

        } else {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();   // do something more meaningful here?
            }
        }

    }

    // should be private, but have not gotten PowerMock unit testing to work yet.
    protected Producer<String, String> setupProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerConnectString);
        props.put("serializer.class", serializerClass);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "sync");
        props.put("batch.size", "1");

        ProducerConfig config = new ProducerConfig(props);
        return new Producer<String, String>(config);
    }
}
