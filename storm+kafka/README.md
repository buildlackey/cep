# Kafka Spout Example


Kafka Spout Integration Test With Local Storm Cluster, and In-Memory Kafka, and Zookeeper Instances


## Description

This example illustrates how to:

    * push messages into Kafka and retrieves those messages with a Kafka/Storm sput.

    * set up your Kafka spout so that it reads all messages in its configured topic from the very first message 
      in that topic (the default behavior), or so that it reads only the messages that are emitted to the topic after
      the spout has been initialized   (to get the latter behavior, specify the --fromCurrent option as shown below.)

    * use the in process Zookeeper server that Storm's LocalCluster seems to 'hardwire' 
      at port 2000 by default. 

          NOTE: there does not seem to be anyway to override LocalCluster's 
          behavior of instantiating its own zookeeper instance by passing in our own Zookeeper
          instance and telling the LocalCluster about that instance via the Map argument 
          passed to LocalCluster(Map)... Oh well.. this example shows a work-around for that.


By keeping all test fixtures in memory (rather than depending on out-of-process servers
being 'somehow' set up before the test) we make it very easy to get the basics of
Kafka Storm integration working in the environments of other developers and/or build systems.



## Building and Running

After downloading the project, cd to the directory in which  this README is located, then issue the 2 commands below
(note that the second command has two variants):

     mvn clean  compile

     mvn exec:java -Dexec.mainClass=TestTopology
            .. or ... 
     mvn exec:java -Dexec.mainClass=TestTopology -Dexec.args="--fromCurrent"



If you see 'SUCCESSFUL COMPLETION' printed out towards the very end, then you know everything is working.



## Implementation Details

The test program pumps a small set of random messages from a Kafka producer thread (started 
at line 77 of Listing 1) to a Kafka Spout consumer, and then asserts that the messages received are identical 
with messages sent (see the verifyResults method of Listing1, starting at line 122.) 


The main method creates an instance of the TestTopology class whose constructor instantiates an 
instance of a Storm LocalCluster. We use the  Zookeeper server in that LocalCluster instance 
since there doesn't seem to be anyway to instantiate  our own Zookeeper and pass that into the 
LocalCluster (as mentioned above.).  Next, we wait for that Zookeeper instance to come up completely
(line 69.)  We then start our Kafka server using the Zookeeper instance  created by LocalCluster. 
This is done by hard coding the default value for the Storm LocalCluster's self launched zookeeper
server to its preferred host/port value (localhost:2000). See lines 74 and 103 of Listing 2. 

The Kafka producer thread  kicked  off at  line 77 of listing 1  emits a batch of 4 messages
BEFORE our topology is even initialized (line 40  of listing 2). After emitting that first batch
the producer thread unleashes the countdown latch 'producerFinishedInitialBatchLatch'. 
This lets the main thread proceed from its wait at line 78.  The next thing the main thread 
does is to Next we set up our test topology, which includes a Kafka spout configured to connect to the Zookeeper 
instance at port 2000. This the same zookeeper instance that we use when we configure 
the Kafka server, so it seems the Kafka spout discovers the 
Kafka broker it needs to connect with via Zookeeper.  Our topology wires the 
Kafka spout to our VerboseCollectorBolt instance whose only job is to dump each tuple it receives
to the console, and collect up each sentence it is transmitted.   In verifyResults (line 102) 
we check to make sure that what the VerboseCollectorBolt has recorded actually matches what 
we know we have sent. 

Note that after we setup our topology (line 80 of Listing 1), we give it a few seconds to launch, then 
we unleash the topologyStartedLatch which causes the KafkaProducer thread to proceed from its wait 
point at line 43 of Listing 2 and emit the second batch of messages.       


Listing 1, TestTopology.java
        1	/*                                                                                                                                           
        2	 * Author: cbedford
        3	 * Date: 10/22/13
        4	 * Time: 8:50 PM
        5	 */
        6	
        7	
        8	import backtype.storm.Config;
        9	import backtype.storm.LocalCluster;
       10	import backtype.storm.spout.SchemeAsMultiScheme;
       11	import backtype.storm.topology.TopologyBuilder;
       12	import storm.kafka.*;
       13	
       14	import java.util.ArrayList;
       15	import java.util.List;
       16	import java.util.Random;
       17	import java.util.concurrent.CountDownLatch;
       18	
       19	public class TestTopology {
       20	
       21	
       22	    final static int MAX_ALLOWED_TO_RUN_MILLISECS = 1000 * 90 /* seconds */;
       23	
       24	    CountDownLatch topologyStartedLatch = new CountDownLatch(1);
       25	
       26	    private static int STORM_KAFKA_FROM_READ_FROM_START = -2;
       27	    private static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
       28	    private static int readFromMode = STORM_KAFKA_FROM_READ_FROM_START;
       29	    private int expectedNumMessages = 8;
       30	
       31	    private static final int SECOND = 1000;
       32	    private static List<String> messagesReceived = new ArrayList<String>();
       33	
       34	    private LocalCluster cluster = new LocalCluster();
       35	
       36	    private static final String TOPIC_NAME = "big-topix-" + new Random().nextInt();
       37	    volatile static boolean finishedCollecting = false;
       38	
       39	    private static String[] sentences = new String[]{
       40	            "one dog9 - saw the fox over the moon",
       41	            "two cats9 - saw the fox over the moon",
       42	            "four bears9 - saw the fox over the moon",
       43	            "five goats9 - saw the fox over the moon",
       44	    };
       45	
       46	    private KafkaProducer kafkaProducer = new  KafkaProducer(sentences, TOPIC_NAME, topologyStartedLatch);
       47	
       48	
       49	    public static void recordRecievedMessage(String msg) {
       50	        synchronized (TestTopology.class) {                 // ensure visibility of list updates between threads
       51	            messagesReceived.add(msg);
       52	        }
       53	    }
       54	
       55	
       56	    public static void main(String[] args) {
       57	        TestTopology testTopology = new TestTopology();
       58	
       59	        if (args.length == 1 && args[0].equals("--fromCurrent")) {
       60	            readFromMode = STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET;
       61	            testTopology.expectedNumMessages  = 4;
       62	        }
       63	
       64	        testTopology.runTest();
       65	    }
       66	
       67	    private void runTest() {
       68	        ServerAndThreadCoordinationUtils.setMaxTimeToRunTimer(MAX_ALLOWED_TO_RUN_MILLISECS);
       69	        ServerAndThreadCoordinationUtils.waitForServerUp("localhost", 2000, 5 * SECOND);   // Wait for zookeeper to come up
       70	
       71	        kafkaProducer.startKafkaServer();
       72	        kafkaProducer.createTopic(TOPIC_NAME);
       73	
       74	        try {
       75	
       76	
       77	            kafkaProducer.startProducer();
       78	            ServerAndThreadCoordinationUtils.await(kafkaProducer.producerFinishedInitialBatchLatch);
       79	
       80	            setupKafkaSpoutAndSubmitTopology();
       81	            try {
       82	                Thread.sleep(5000);                 // Would be nice to have a call back inform us when ready
       83	            } catch (InterruptedException e) {
       84	                e.printStackTrace();
       85	            }
       86	            ServerAndThreadCoordinationUtils.countDown(topologyStartedLatch);
       87	
       88	            awaitResults();
       89	        } catch (InterruptedException e) {
       90	            e.printStackTrace();
       91	        }
       92	
       93	        verifyResults();
       94	        shutdown();
       95	        System.out.println("SUCCESSFUL COMPLETION");
       96	        System.exit(0);
       97	    }
       98	
       99	
      100	
      101	    private void awaitResults() {
      102	        while (!finishedCollecting) {
      103	            try {
      104	                Thread.sleep(500);
      105	            } catch (InterruptedException e) {
      106	                e.printStackTrace();
      107	            }
      108	        }
      109	
      110	        // Sleep another couple of seconds in case any more messages than expected come into the bolt.
      111	        // In this case the bolt should throw a fatal error
      112	        try {
      113	            Thread.sleep(2000);
      114	        } catch (InterruptedException e) {
      115	            e.printStackTrace();
      116	        }
      117	
      118	
      119	        System.out.println("after await");
      120	    }
      121	
      122	    private void verifyResults() {
      123	        synchronized (TestTopology.class) {                 // ensure visibility of list updates between threads
      124	            int count = 0;
      125	            for (String msg : messagesReceived) {
      126	                if (msg.contains("cat") || msg.contains("dog") || msg.contains("bear") || msg.contains("goat")) {
      127	                    count++;
      128	                }
      129	            }
      130	            if (count != expectedNumMessages) {
      131	                System.out.println(">>>>>>>>>>>>>>>>>>>>FAILURE -   Did not receive expected messages");
      132	                System.exit(-1);
      133	            }
      134	
      135	        }
      136	    }
      137	
      138	    private void setupKafkaSpoutAndSubmitTopology() throws InterruptedException {
      139	        BrokerHosts brokerHosts = new ZkHosts("localhost:2000");
      140	
      141	        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
      142	        kafkaConfig.forceStartOffsetTime(readFromMode  /* either earliest or current offset */);
      143	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
      144	
      145	
      146	        TopologyBuilder builder = new TopologyBuilder();
      147	        builder.setSpout("words", new KafkaSpout(kafkaConfig), 1);
      148	        VerboseCollectorBolt bolt = new VerboseCollectorBolt(expectedNumMessages);
      149	        builder.setBolt("print", bolt).shuffleGrouping("words");
      150	
      151	
      152	        Config config = new Config();
      153	
      154	        cluster.submitTopology("kafka-test", config, builder.createTopology());
      155	    }
      156	
      157	    private void shutdown() {
      158	        cluster.shutdown();
      159	        kafkaProducer.shutdown();
      160	    }
      161	
      162	
      163	
      164	}
      165	




Listing 2, KafkaProducer.java

       1	import java.util.concurrent.CountDownLatch;                                                                  
       2	import com.google.common.io.Files;
       3	import kafka.admin.CreateTopicCommand;
       4	import kafka.javaapi.producer.Producer;
       5	import kafka.producer.KeyedMessage;
       6	import kafka.producer.ProducerConfig;
       7	import kafka.server.KafkaConfig;
       8	import kafka.server.KafkaServer;
       9	import kafka.utils.MockTime;
      10	
      11	import java.io.File;
      12	import java.util.Properties;
      13	
      14	
      15	public class KafkaProducer {
      16	
      17	    private KafkaServer kafkaServer = null;
      18	    private final String topicName;
      19	
      20	
      21	    CountDownLatch topologyStartedLatch;
      22	    public CountDownLatch producerFinishedInitialBatchLatch = new CountDownLatch(1);
      23	
      24	
      25	    Producer<String, String> producer;
      26	
      27	    private String[] sentences;
      28	
      29	    KafkaProducer(String[] sentences, String topicName, CountDownLatch topologyStartedLatch) {
      30	        this.sentences = sentences;
      31	        this.topicName = topicName;
      32	        this.topologyStartedLatch = topologyStartedLatch;
      33	    }
      34	
      35	    public Thread startProducer() {
      36	        Thread sender = new Thread(
      37	                new Runnable() {
      38	                    @Override
      39	                    public void run() {
      40	                        emitBatch();
      41	                        ServerAndThreadCoordinationUtils.
      42	                                countDown(producerFinishedInitialBatchLatch);
      43	                        ServerAndThreadCoordinationUtils.
      44	                                await(topologyStartedLatch);
      45	                        emitBatch();  // emit second batch after we know topology is up
      46	                    }
      47	                },
      48	                "producerThread"
      49	        );
      50	        sender.start();
      51	        return sender;
      52	    }
      53	
      54	    private void emitBatch() {
      55	        Properties props = new Properties();
      56	        props.put("metadata.broker.list", "localhost:9092");
      57	        props.put("serializer.class", "kafka.serializer.StringEncoder");
      58	        props.put("request.required.acks", "1");
      59	        ProducerConfig config = new ProducerConfig(props);
      60	        Producer<String, String> producer = new Producer<String, String>(config);
      61	
      62	        for (String sentence : sentences) {
      63	            KeyedMessage<String, String> data =
      64	                    new KeyedMessage<String, String>(topicName, sentence);
      65	            producer.send(data);
      66	        }
      67	        producer.close();
      68	
      69	    }
      70	
      71	    public void createTopic(String topicName) {
      72	        String[] arguments = new String[8];
      73	        arguments[0] = "--zookeeper";
      74	        arguments[1] = "localhost:2000";
      75	        arguments[2] = "--replica";
      76	        arguments[3] = "1";
      77	        arguments[4] = "--partition";
      78	        arguments[5] = "1";
      79	        arguments[6] = "--topic";
      80	        arguments[7] = topicName;
      81	
      82	        CreateTopicCommand.main(arguments);
      83	    }
      84	
      85	    public void startKafkaServer() {
      86	        File tmpDir = Files.createTempDir();
      87	        Properties props = createProperties(tmpDir.getAbsolutePath(), 9092, 1);
      88	        KafkaConfig kafkaConfig = new KafkaConfig(props);
      89	
      90	        kafkaServer = new KafkaServer(kafkaConfig, new MockTime());
      91	        kafkaServer.startup();
      92	    }
      93	
      94	    public void shutdown() {
      95	        kafkaServer.shutdown();
      96	    }
      97	
      98	    private Properties createProperties(String logDir, int port, int brokerId) {
      99	        Properties properties = new Properties();
     100	        properties.put("port", port + "");
     101	        properties.put("broker.id", brokerId + "");
     102	        properties.put("log.dir", logDir);
     103	        properties.put("zookeeper.connect", "localhost:2000");         // Uses zookeeper created by LocalCluster
     104	        return properties;
     105	    }
     106	
     107	}









## Current Issues

I am not confident that the Maven dependencies that I have put together are optimal, but they seem to work for now. 

## Acknowledgements 

Got a good start from this github repo: https://github.com/wurstmeister



