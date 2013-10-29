/*
 * Author: cbedford
 * Date: 10/28/13
 * Time: 6:07 PM
 */


import java.util.concurrent.CountDownLatch;
/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import com.google.common.io.Files;
import kafka.admin.CreateTopicCommand;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;

import java.io.File;
import java.util.Properties;


public class KafkaProducer {

    private KafkaServer kafkaServer = null;
    private final String topicName;


    CountDownLatch topologyStartedLatch;
    public CountDownLatch producerFinishedInitialBatchLatch = new CountDownLatch(1);


    Producer<String, String> producer;

    private String[] sentences;

    KafkaProducer(String[] sentences, String topicName, CountDownLatch topologyStartedLatch) {
        this.sentences = sentences;
        this.topicName = topicName;
        this.topologyStartedLatch = topologyStartedLatch;
    }

    public Thread startProducer() {
        Thread sender = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        emitBatch();
                        ServerAndThreadCoordinationUtils.
                                countDown(producerFinishedInitialBatchLatch);
                        ServerAndThreadCoordinationUtils.
                                await(topologyStartedLatch);
                        emitBatch();  // emit second batch after we know topology is up
                    }
                },
                "producerThread"
        );
        sender.start();
        return sender;
    }

    private void emitBatch() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        for (String sentence : sentences) {
            KeyedMessage<String, String> data =
                    new KeyedMessage<String, String>(topicName, sentence);
            producer.send(data);
        }
        producer.close();

    }

    public void createTopic(String topicName) {
        String[] arguments = new String[8];
        arguments[0] = "--zookeeper";
        arguments[1] = "localhost:2000";
        arguments[2] = "--replica";
        arguments[3] = "1";
        arguments[4] = "--partition";
        arguments[5] = "1";
        arguments[6] = "--topic";
        arguments[7] = topicName;

        CreateTopicCommand.main(arguments);
    }

    public void startKafkaServer() {
        File tmpDir = Files.createTempDir();
        Properties props = createProperties(tmpDir.getAbsolutePath(), 9092, 1);
        KafkaConfig kafkaConfig = new KafkaConfig(props);

        kafkaServer = new KafkaServer(kafkaConfig, new MockTime());
        kafkaServer.startup();
    }

    public void shutdown() {
        kafkaServer.shutdown();
    }

    private Properties createProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("broker.id", brokerId + "");
        properties.put("log.dir", logDir);
        properties.put("zookeeper.connect", "localhost:2000");         // Uses zookeeper created by LocalCluster
        return properties;
    }

}
