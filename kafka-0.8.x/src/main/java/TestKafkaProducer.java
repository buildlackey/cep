/*
 * Author: cbedford
 * Date: 10/20/13
 * Time: 8:54 PM
 */


import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.netflix.curator.test.TestingServer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.VerifiableProperties;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;


class TestKafkaProducer {
    private String topic = "";
    private String zkConnectString = "";
    private List<String> messages = null;
    private List<String> messagesReceived = new ArrayList<String>();
    private Producer<String, String> producer;
    private KafkaServer kafkaServer;
    private Thread kafkaMessageReceiverThread;

    private static final String RANDOM_GROUP_ID = "RANDOM-GROUP-ID";

    public static void main(String[] args) {
        TestKafkaProducer tkp = null;

        boolean success = false;

        try (TestingServer zookeeperTestServer =  new TestingServer())  {

            final String theTopic = "someTopic-" + new Random().nextInt();

            tkp = new TestKafkaProducer(
                    theTopic,
                    "localhost:" + zookeeperTestServer.getPort(),
                    4400);

            tkp.sendMessages();
            tkp.consumeMessages();

            try {               // Give consumer some time...
                tkp.shutdownConsumers();
                Thread.sleep(1000);
                tkp.kafkaMessageReceiverThread.join();
                tkp.shutdown();
            } catch (Exception e) {
                System.out.println("Error in shut down. we will ignore it as long as our messages came through");
                e.printStackTrace();
            }

            String got = StringUtils.join(tkp.messagesReceived, "+");
            String expected = StringUtils.join(tkp.messages, "+");
            if (got.equals(expected)) {
                success = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (! success) {
            throw new RuntimeException("oh rats... we failed");
        }
        System.out.println("SUCCESS -- WE ARE HAPPY !...");
    }

    private void consumeMessages() {
        final ConsumerConnector   consumer =
                kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        final Map<String, Integer> topicCountMap =
                ImmutableMap.of(topic, 1);
        final StringDecoder decoder =
                new StringDecoder(new VerifiableProperties());
        final Map<String, List<KafkaStream<String,String>>>  consumerMap =
                consumer.createMessageStreams(topicCountMap, decoder,  decoder);
        final KafkaStream<String,String> stream =
                consumerMap.get(topic).get(0);
        final ConsumerIterator<String,String> iterator = stream.iterator();

        kafkaMessageReceiverThread = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        while (iterator.hasNext()) {
                            String msg = iterator.next().message();
                            msg = msg == null ? "<null>" : msg;
                            System.out.println("got message" + msg);
                            if (msg.equals("SHUTDOWN")) {
                                consumer.shutdown();
                                return;
                            }
                            messagesReceived.add(msg);
                        }
                    }
                },
                "kafkaMessageReceiverThread"
        );
        kafkaMessageReceiverThread.start();

    }


    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", this.zkConnectString);
        props.put("group.id", RANDOM_GROUP_ID);
        props.put("zk.sessiontimeout.ms", "400");
        props.put("zk.synctime.ms", "200");
        props.put("autocommit.interval.ms", "1000");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        return new ConsumerConfig(props);

    }

    public void shutdownConsumers() {
        sendMessage("SHUTDOWN");
    }


    public void shutdown() {
        producer.close();
        try {               // Give producer some time...
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();
    }


    TestKafkaProducer(String topic, String zkConnectString, int numRandomMessages) throws IOException {
        final Random generator = new Random();

        if (numRandomMessages <= 0) {
            throw new RuntimeException("no messages defined for test");
        }

        messages = new ArrayList<String>();
        for (int i = 0; i < numRandomMessages; i++) {
            int num1 = Math.abs(generator.nextInt());
            int num2 = Math.abs(generator.nextInt());
            String messageToSend = num1 + ":-(a)-" + num2;
            messages.add(messageToSend);
        }


        this.topic = topic;

        this.zkConnectString = zkConnectString;
        initProducer(zkConnectString);
    }


    public void sendMessages() throws IOException {
        for (String msg : messages) {
            sendMessage(msg);
        }
    }

    private void sendMessage(String msg) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
        producer.send(data);
    }

    private void initProducer(String zkConnectString) throws IOException {
        kafkaServer =  startKafkaServer();
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        props.put("batch.size", "1");
        ProducerConfig config = new ProducerConfig(props);

        producer = new Producer<String, String>(config);
    }

    private KafkaServer startKafkaServer() {
        File tmpDir =  Files.createTempDir();
        Properties props = createProperties(tmpDir.getAbsolutePath(), 9092, 1);
        KafkaConfig kafkaConfig = new KafkaConfig(props);

        kafkaServer = new KafkaServer(kafkaConfig, new MockTime());

        kafkaServer.startup();
        return kafkaServer;
    }



    private Properties createProperties(String logDir, int port, int brokerId) {
          Properties properties = new Properties();
          properties.put("port", port + "");
          properties.put("broker.id", brokerId + "");
          properties.put("log.dir", logDir);
          properties.put("zookeeper.connect", this.zkConnectString);
          return properties;
      }


}
