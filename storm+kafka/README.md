# Kafka Spout Example


Kafka Spout Integration Test With Local Storm Cluster, and In-Memory Kafka, and Zookeeper Instances


## Description

This example illustrates how to:

    * push messages into Kafka and retrieves those messages with a Kafka/Storm sput.

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

After downloading the project, cd to the directory in which  this README is located, then issue the 2 commands:

     mvn clean 

     mvn exec:java -Dexec.mainClass=TestTopology

If you see 'SUCCESSFUL COMPLETION' printed out towards the very end, then you know everything is working.



## Implementation Details

The test program pumps a small set of random messages from a Kafka producer thread (line 148, below) 
to a Kafka Spout consumer, and then asserts that the messages received are identical 
with messages sent (lines 135-145, below.) 


The main method creates an instance of the TestTopology class whose constructor instantiates an 
instance of a Storm LocalCluster. We use the  Zookeeper server in that LocalCluster instance 
since there doesn't seem to be anyway to instantiate  our own Zookeeper and pass that into the 
LocalCluster (as mentioned above.).  Next, we wait for that Zookeeper instance to come up completely
(line 86.)  We then start our Kafka server (using the Zookeeper instance  created by LocalCluster) at
line 197, and create a random topic at line 89.  

We kick off the producer thread at line 93, then make sure it has finished by joining it at line 94.

Next we set up our topology, which includes a Kafka spout configured to connect to the Zookeeper 
instance at port 2000. This the same zookeeper
instance that we use when we configure the Kafka server, so it seems the Kafka spout discovers the 
Kafka broker it needs to connect with via Zookeeper (line 174).  Our topology wires the 
Kafka spout to our VerboseCollectorBolt instance whose only job is to dump each tuple it receives
to the console, and collect up each sentence it is transmitted.   In verifyResults (line 102) 
we check to make sure that what the VerboseCollectorBolt has recorded actually matches what 
we know we have sent. 


Listing 1, Storm/Kafka Spout 

     1	import backtype.storm.Config;
     2	import backtype.storm.LocalCluster;
     3	import backtype.storm.spout.SchemeAsMultiScheme;
     4	import backtype.storm.topology.BasicOutputCollector;
     5	import backtype.storm.topology.OutputFieldsDeclarer;
     6	import backtype.storm.topology.TopologyBuilder;
     7	import backtype.storm.topology.base.BaseBasicBolt;
     8	import backtype.storm.tuple.Tuple;
     9	import kafka.javaapi.producer.Producer;
    10	import kafka.producer.KeyedMessage;
    11	import kafka.producer.ProducerConfig;
    12	import kafka.utils.MockTime;
    13	import storm.kafka.*;
    14	
    15	import kafka.server.KafkaConfig;
    16	import kafka.server.KafkaServer;
    17	import kafka.admin.CreateTopicCommand;
    18	
    19	import java.io.*;
    20	import java.net.Socket;
    21	import java.util.ArrayList;
    22	import java.util.List;
    23	import java.util.Properties;
    24	import java.util.Random;
    25	import java.util.concurrent.Callable;
    26	
    27	import com.google.common.io.Files;
    28	
    29	import static com.jayway.awaitility.Awaitility.await;
    30	import static java.util.concurrent.TimeUnit.SECONDS;
    31	
    32	public class TestTopology {
    33	
    34	    private static final int SECOND = 1000;
    35	    private static List<String> messagesReceived = new ArrayList<String>();
    36	
    37	    private LocalCluster  cluster = new LocalCluster();
    38	    private KafkaServer kafkaServer = null;
    39	
    40	    private static final String TOPIC_NAME = "big-topix-" + new Random().nextInt();
    41	    volatile static boolean finishedCollecting = false;
    42	
    43	    private static String[] sentences = new String[]{
    44	            "one dog9 - saw the fox over the moon",
    45	            "two cats9 - saw the fox over the moon",
    46	            "four bears9 - saw the fox over the moon",
    47	            "five goats9 - saw the fox over the moon",
    48	    };
    49	
    50	    public static void recordRecievedMessage(String msg) {
    51	        synchronized (TestTopology.class) {                 // ensure visibility of list updates between threads
    52	            messagesReceived.add(msg);
    53	        }
    54	    }
    55	
    56	
    57	    public static class VerboseCollectorBolt extends BaseBasicBolt {
    58	
    59	        private int expectedNumMessages;
    60	        private int countReceivedMessages = 0;
    61	
    62	        VerboseCollectorBolt(int expectedNumMessages) {
    63	            this.expectedNumMessages = expectedNumMessages;
    64	        }
    65	
    66	        @Override
    67	        public void declareOutputFields(OutputFieldsDeclarer declarer) {
    68	        }
    69	
    70	        @Override
    71	        public void execute(Tuple tuple, BasicOutputCollector collector) {
    72	            final String  msg = tuple.toString();
    73	
    74	            System.out.println(">>>>>>>>>>>>>"  + msg);
    75	            countReceivedMessages++;
    76	            recordRecievedMessage(msg);
    77	            if (countReceivedMessages == expectedNumMessages) {
    78	                finishedCollecting = true;
    79	            }
    80	        }
    81	
    82	    }
    83	
    84	    public static void main(String[] args) {
    85	        TestTopology topo = new TestTopology();
    86	        waitForServerUp("localhost", 2000, 5 * SECOND );            // Wait for zookeeper to come up
    87	        topo.startKafkaServer();
    88	
    89	        createTopic();
    90	
    91	
    92	        try {
    93	            Thread producer = topo.startProducer();
    94	            producer.join();
    95	
    96	            topo.setupKafkaSpoutAndSubmitTopology();
    97	            topo.awaitResults();
    98	        } catch (InterruptedException e) {
    99	            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    100	       }
    101	
    102	       topo.verifyResults();
    103	       topo.shutdown();
    104	       System.out.println("SUCCESSFUL COMPLETION");
    105	       System.exit(0);
    106	   }
    107	
    108	
    109	   private static void createTopic() {
    110	       String[] arguments = new String[8];
    111	       arguments[0] = "--zookeeper";
    112	       arguments[1] = "localhost:2000";
    113	       arguments[2] = "--replica";
    114	       arguments[3] = "1";
    115	       arguments[4] = "--partition";
    116	       arguments[5] = "2";
    117	       arguments[6] = "--topic";
    118	       arguments[7] = TOPIC_NAME;
    119	
    120	       CreateTopicCommand.main(arguments);
    121	   }
    122	
    123	
    124	
    125	   private void awaitResults() {
    126	       await().atMost(20, SECONDS).until(new Callable<Boolean>() {
    127	           @Override
    128	           public Boolean call() throws Exception {
    129	               return finishedCollecting;
    130	           }
    131	       });
    132	       System.out.println("after await");
    133	   }
    134	
    135	   private void verifyResults() {
    136	       int count = 0;
    137	       for (String msg : messagesReceived) {
    138	           if (msg.contains("cat") || msg.contains("dog") || msg.contains("bear") || msg.contains("goat")) {
    139	               count++;
    140	           }
    141	       }
    142	       if ( count != 4) {
    143	           System.out.println(">>>>>>>>>>>>>>>>>>>>FAILURE -   Did not receive expected messages");
    144	           System.exit(-1);
    145	       }
    146	   }
    147	
    148	   private Thread startProducer() {
    149	       Thread sender = new Thread(
    150	               new Runnable() {
    151	                   @Override
    152	                   public void run() {
    153	                       Properties props = new Properties();
    154	                       props.put("metadata.broker.list", "localhost:9092");
    155	                       props.put("serializer.class", "kafka.serializer.StringEncoder");
    156	                       props.put("request.required.acks", "1");
    157	                       ProducerConfig config = new ProducerConfig(props);
    158	                       Producer<String, String> producer = new Producer<String, String>(config);
    159	
    160	                       for (String sentence : sentences) {
    161	                           KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC_NAME, sentence);
    162	                           producer.send(data);
    163	                       }
    164	                       producer.close();
    165	                   }
    166	               },
    167	               "producerThread"
    168	       );
    169	       sender.start();
    170	       return sender;
    171	   }
    172	
    173	   private void setupKafkaSpoutAndSubmitTopology() throws InterruptedException {
    174	       BrokerHosts brokerHosts = new ZkHosts("localhost:2000");
    175	
    176	       SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, TOPIC_NAME, "", "storm");
    177	       kafkaConfig.forceStartOffsetTime(-2 /* earliest offset */);
    178	       kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    179	
    180	
    181	       TopologyBuilder builder = new TopologyBuilder();
    182	       builder.setSpout("words", new KafkaSpout(kafkaConfig), 10);
    183	       builder.setBolt("print", new VerboseCollectorBolt(4)).shuffleGrouping("words");
    184	
    185	
    186	       Config config = new Config();
    187	
    188	       cluster.submitTopology("kafka-test", config, builder.createTopology());
    189	   }
    190	
    191	   private void shutdown() {
    192	       cluster.shutdown();
    193	       kafkaServer.shutdown();
    194	   }
    195	
    196	
    197	   private void startKafkaServer() {
    198	       File tmpDir =  Files.createTempDir();
    199	       Properties props = createProperties(tmpDir.getAbsolutePath(), 9092, 1);
    200	       KafkaConfig kafkaConfig = new KafkaConfig(props);
    201	
    202	       kafkaServer = new KafkaServer(kafkaConfig, new MockTime());
    203	       kafkaServer.startup();
    204	   }
    205	
    206	
    207	   private Properties createProperties(String logDir, int port, int brokerId) {
    208	       Properties properties = new Properties();
    209	       properties.put("port", port + "");
    210	       properties.put("broker.id", brokerId + "");
    211	       properties.put("log.dir", logDir);
    212	       properties.put("zookeeper.connect", "localhost:2000");         // Uses zookeeper created by LocalCluster
    213	       return properties;
    214	   }
    215	
    216	
    217	
    218	
    219	   public static String send4LetterWord(String host, int port, String cmd)
    220	       throws IOException
    221	   {
    222	       System.out.println("connecting to " + host + " " + port);
    223	       Socket sock = new Socket(host, port);
    224	       BufferedReader reader = null;
    225	       try {
    226	           OutputStream outstream = sock.getOutputStream();
    227	           outstream.write(cmd.getBytes());
    228	           outstream.flush();
    229	           // this replicates NC - close the output stream before reading
    230	           sock.shutdownOutput();
    231	
    232	           reader =
    233	               new BufferedReader(
    234	                       new InputStreamReader(sock.getInputStream()));
    235	           StringBuilder sb = new StringBuilder();
    236	           String line;
    237	           while((line = reader.readLine()) != null) {
    238	               sb.append(line + "\n");
    239	           }
    240	           return sb.toString();
    241	       } finally {
    242	           sock.close();
    243	           if (reader != null) {
    244	               reader.close();
    245	           }
    246	       }
    247	   }
    248	
    249	   public static boolean waitForServerUp(String host, int port, long timeout) {
    250	       long start = System.currentTimeMillis();
    251	       while (true) {
    252	           try {
    253	               // if there are multiple hostports, just take the first one
    254	               String result = send4LetterWord(host, port, "stat");
    255	               System.out.println("result of send: " + result);
    256	               if (result.startsWith("Zookeeper version:")) {
    257	                   return true;
    258	               }
    259	           } catch (IOException e) {
    260	               // ignore as this is expected
    261	               System.out.println("server " + host  +  ":" + port + " not up " + e);
    262	           }
    263	
    264	           if (System.currentTimeMillis() > start + timeout) {
    265	               break;
    266	           }
    267	           try {
    268	               Thread.sleep(250);
    269	           } catch (InterruptedException e) {
    270	               // ignore
    271	           }
    272	       }
    273	       return false;
    274	   }
    275	
    276 }	
    277	



## Current Issues

I am not confident that the Maven dependencies that I have put together are optimal, but they seem to work for now. 

## Acknowledgements 

Got a good start from this github repo: https://github.com/wurstmeister



