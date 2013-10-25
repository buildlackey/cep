/*
 * Author: cbedford
 * Date: 10/22/13
 * Time: 8:50 PM
 */


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.MockTime;
import storm.kafka.*;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.admin.CreateTopicCommand;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import com.google.common.io.Files;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestTopology {

    private static final int SECOND = 1000;
    private static List<String> messagesReceived = new ArrayList<String>();

    private LocalCluster  cluster = new LocalCluster();
    private KafkaServer kafkaServer = null;

    private static final String TOPIC_NAME = "big-topix-" + new Random().nextInt();
    volatile static boolean finishedCollecting = false;

    private static String[] sentences = new String[]{
            "one dog9 - saw the fox over the moon",
            "two cats9 - saw the fox over the moon",
            "four bears9 - saw the fox over the moon",
            "five goats9 - saw the fox over the moon",
    };

    public static void recordRecievedMessage(String msg) {
        synchronized (TestTopology.class) {                 // ensure visibility of list updates between threads
            messagesReceived.add(msg);
        }
    }


    public static class VerboseCollectorBolt extends BaseBasicBolt {

        private int expectedNumMessages;
        private int countReceivedMessages = 0;

        VerboseCollectorBolt(int expectedNumMessages) {
            this.expectedNumMessages = expectedNumMessages;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            final String  msg = tuple.toString();

            System.out.println(">>>>>>>>>>>>>"  + msg);
            countReceivedMessages++;
            recordRecievedMessage(msg);
            if (countReceivedMessages == expectedNumMessages) {
                finishedCollecting = true;
            }
        }

    }

    public static void main(String[] args) {
        TestTopology topo = new TestTopology();
        waitForServerUp("localhost", 2000, 5 * SECOND );            // Wait for zookeeper to come up
        topo.startKafkaServer();

        createTopic();


        try {
            Thread producer = topo.startProducer();
            producer.join();

            topo.setupKafkaSpoutAndSubmitTopology();
            topo.awaitResults();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        topo.verifyResults();
        topo.shutdown();
        System.out.println("SUCCESSFUL COMPLETION");
        System.exit(0);
    }


    private static void createTopic() {
        String[] arguments = new String[8];
        arguments[0] = "--zookeeper";
        arguments[1] = "localhost:2000";
        arguments[2] = "--replica";
        arguments[3] = "1";
        arguments[4] = "--partition";
        arguments[5] = "2";
        arguments[6] = "--topic";
        arguments[7] = TOPIC_NAME;

        CreateTopicCommand.main(arguments);
    }



    private void awaitResults() {
        await().atMost(20, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return finishedCollecting;
            }
        });
        System.out.println("after await");
    }

    private void verifyResults() {
        int count = 0;
        for (String msg : messagesReceived) {
            if (msg.contains("cat") || msg.contains("dog") || msg.contains("bear") || msg.contains("goat")) {
                count++;
            }
        }
        if ( count != 4) {
            System.out.println(">>>>>>>>>>>>>>>>>>>>FAILURE -   Did not receive expected messages");
            System.exit(-1);
        }
    }

    private Thread startProducer() {
        Thread sender = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        Properties props = new Properties();
                        props.put("metadata.broker.list", "localhost:9092");
                        props.put("serializer.class", "kafka.serializer.StringEncoder");
                        props.put("request.required.acks", "1");
                        ProducerConfig config = new ProducerConfig(props);
                        Producer<String, String> producer = new Producer<String, String>(config);

                        for (String sentence : sentences) {
                            KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC_NAME, sentence);
                            producer.send(data);
                        }
                        producer.close();
                    }
                },
                "producerThread"
        );
        sender.start();
        return sender;
    }

    private void setupKafkaSpoutAndSubmitTopology() throws InterruptedException {
        BrokerHosts brokerHosts = new ZkHosts("localhost:2000");

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
        kafkaConfig.forceStartOffsetTime(-2 /* earliest offset */);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("print", new VerboseCollectorBolt(4)).shuffleGrouping("words");


        Config config = new Config();

        cluster.submitTopology("kafka-test", config, builder.createTopology());
    }

    private void shutdown() {
        cluster.shutdown();
        kafkaServer.shutdown();
    }


    private void startKafkaServer() {
        File tmpDir =  Files.createTempDir();
        Properties props = createProperties(tmpDir.getAbsolutePath(), 9092, 1);
        KafkaConfig kafkaConfig = new KafkaConfig(props);

        kafkaServer = new KafkaServer(kafkaConfig, new MockTime());
        kafkaServer.startup();
    }


    private Properties createProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("broker.id", brokerId + "");
        properties.put("log.dir", logDir);
        properties.put("zookeeper.connect", "localhost:2000");         // Uses zookeeper created by LocalCluster
        return properties;
    }




    public static String send4LetterWord(String host, int port, String cmd)
        throws IOException
    {
        System.out.println("connecting to " + host + " " + port);
        Socket sock = new Socket(host, port);
        BufferedReader reader = null;
        try {
            OutputStream outstream = sock.getOutputStream();
            outstream.write(cmd.getBytes());
            outstream.flush();
            // this replicates NC - close the output stream before reading
            sock.shutdownOutput();

            reader =
                new BufferedReader(
                        new InputStreamReader(sock.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
            return sb.toString();
        } finally {
            sock.close();
            if (reader != null) {
                reader.close();
            }
        }
    }

    public static boolean waitForServerUp(String host, int port, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                // if there are multiple hostports, just take the first one
                String result = send4LetterWord(host, port, "stat");
                System.out.println("result of send: " + result);
                if (result.startsWith("Zookeeper version:")) {
                    return true;
                }
            } catch (IOException e) {
                // ignore as this is expected
                System.out.println("server " + host  +  ":" + port + " not up " + e);
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

}
