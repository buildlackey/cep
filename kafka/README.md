# Kafka Produce/Consumer Example 

A Simple Kafka Produce/Consumer Example With In-Memory Kafka and Zookeeper Test Fixture Servers


## Description

This example illustrates how to:

    * unit test message passing between Kafka producers and consumers 
      using basic String serialization.  

    * use of Netflix's curator API to instantiate an in-process zookeeper 
      server, together with an in-memory instance of the 
      kafka.server.KafkaServer class 

    * ensure that all threads launched by Kafka and zookeeper are cleanly 
      shutdown  (this seem to be working pretty well so far.)



By keeping all test fixtures in memory (rather than depending on out-of-process servers
being 'somehow' set up before the test) we make it very easy to get the basics of
Kafka working in the environments of other developers and/or build systems.


The main problem with the initial cut of this test program is that I had to use some very strange
dependencies in my maven pom.xml in order to be able to get everything working through a public
repo. (See 'Current Issues', below.)




## Building and Running

After downloading the project, cd to the directory in which this README is located, then issue the 2 commands:

     mvn clean install

     mvn exec:java -Dexec.mainClass=TestKafkaProducer

If you see 'SUCCESS' printed out towards the very end, then you know everything is working.



## Implementation Details

The test program pumps a small set of random messages from a producer to a consumer, and 
asserts that the messages received are identical with messages sent (lines 22-26, below.) 


The main method creates an instance of the Netflix's curator API TestingServer class
with default parameters which cause it to select a random unused port, as well as a 
random temp directory for the zookeeper files (line 6).  On line 12 we interogate 
zookeeperTestServer for its port to construct the zookeeper connect string 
("zk.connect" property) used by both the Producer (created in initProducer) 
and the consumer (whose connect properties are created in createConsumerConfig, at line 70.)




Listing 1, Main Routine 

     1	     public static void main(String[] args) {
     2	        TestKafkaProducer tkp = null;
     3	
     4	        boolean success = false;
     5	
     6	        try (TestingServer zookeeperTestServer =  new TestingServer())  {
     7	
     8	            final String theTopic = "someTopic-" + new Random().nextInt();
     9	
    10	            tkp = new TestKafkaProducer(
    11	                    theTopic,
    12	                    "localhost:" + zookeeperTestServer.getPort(),
    13	                    10);
    14	
    15	            tkp.sendMessages();
    16	
    17	            tkp.consumeMessages();
    18	            tkp.shutdownConsumers();
    19	            tkp.kafkaMessageReceiverThread.join();
    20	            tkp.shutdown();
    21	
    22	            String got = StringUtils.join(tkp.messagesReceived, "+");
    23	            String expected = StringUtils.join(tkp.messages, "+");
    24	            if (got.equals(expected)) {
    25	                success = true;
    26	            }
    27	        } catch (Exception e) {
    28	            e.printStackTrace();
    29	        }
    30	        if (! success) {
    31	            throw new RuntimeException("oh rats... we failed");
    32	        }
    33	        System.out.println("SUCCESS - WE ARE HAPPY !...");
    34	    }

        ....

    38	private void consumeMessages() {
    39	    final ConsumerConnector   consumer =
    40	            kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    41	    final Map<String, Integer> topicCountMap = ImmutableMap.of(topic, 1);
    42	    final Map<String, List<KafkaStream<String>>> consumerMap;
    43	    consumerMap = consumer.createMessageStreams(topicCountMap, new StringDecoder());
    44	
    45	    final KafkaStream<String> stream = consumerMap.get(topic).get(0);
    46	    final ConsumerIterator<String> iterator = stream.iterator();
    47	
    48	    kafkaMessageReceiverThread = new Thread(
    49	            new Runnable() {
    50	                @Override
    51	                public void run() {
    52	                    while (iterator.hasNext()) {
    53	                        String msg = iterator.next().message();
    54	                        msg = msg == null ? "<null>" : msg;
    55	                        System.out.println("got message" + msg);
    56	                        if (msg.equals("SHUTDOWN")) {
    57	                            consumer.shutdown();
    58	                            return;
    59	                        }
    60	                        messagesReceived.add(msg);
    61	                    }
    62	                }
    63	            },
    64	            "kafkaMessageReceiverThread"
    65	    );
    66	    kafkaMessageReceiverThread.start();
    67	}
    68	
    69	
    70	private ConsumerConfig createConsumerConfig() {
    71	    Properties props = new Properties();
    72	    props.put("zk.connect", this.zkConnectString);
    73	    props.put("groupid", RANDOM_GROUP_ID);
    74	    props.put("zk.sessiontimeout.ms", "400");
    75	    props.put("zk.synctime.ms", "200");
    76	    props.put("autocommit.interval.ms", "1000");
    77	    props.put("serializer.class", "kafka.serializer.StringEncoder");
    78	
    79	    return new ConsumerConfig(props);
    80	}
    81	


The TestKafkaProducer constructor (line 83) sets up the producer in initProducer (line 117), 
and an array of random strings to send to the consumer (stored in the 'messages' 
member variable, at line 95)  These messages are sent via sendMessages() at line 15
(see Listing 1, above.) 


Listing 2, TestKafkaProducer Constructor 


        83	TestKafkaProducer(String topic, String zkConnectString, int numRandomMessages) throws IOException {
        84	    final Random generator = new Random();
        85	
        86	    if (numRandomMessages <= 0) {
        87	        throw new RuntimeException("no messages defined for test");
        88	    }
        89	
        90	    messages = new ArrayList<String>();
        91	    for (int i = 0; i < numRandomMessages; i++) {
        92	        int num1 = Math.abs(generator.nextInt());
        93	        int num2 = Math.abs(generator.nextInt());
        94	        String messageToSend = num1 + ":-(a)-" + num2;
        95	        messages.add(messageToSend);
        96	    }
        97	
        98	
        99	    this.topic = topic;
       100	
       101	    this.zkConnectString = zkConnectString;
       102	    initProducer(zkConnectString);
       103	}
       104	
       105	
       106	public void sendMessages() throws IOException {
       107	    for (String msg : messages) {
       108	        sendMessage(msg);
       109	    }
       110	}
       111	
       112	private void sendMessage(String msg) {
       113	    ProducerData<String, String> data = new ProducerData<String, String>(topic, msg);
       114	    producer.send(data);
       115	}
       116	
       117	private void initProducer(String zkConnectString) throws IOException {
       118	    kafkaServer =  startKafkaServer();
       119	
       120	
       121	    Properties props = new Properties();
       122	    props.put("zk.connect", zkConnectString);
       123	    props.put("serializer.class", "kafka.serializer.StringEncoder");
       124	    props.put("producer.type", "async");
       125	    props.put("batch.size", "1");
       126	    ProducerConfig config = new ProducerConfig(props);
       127	
       128	    producer = new Producer<String, String>(config);
       129	}



Note that the sequence of events we follow after sending the messages is to launch a thread that 
consumes the messages (in consumeMessages at  line 48.)  We then get the consumer to shutdown cleanly 
by sending it a 'poison pill'
( see: http://books.google.com/books?id=EK43StEVfJIC&pg=PT172&lpg=PT172&dq=shut+down+poison+pill+queue&source=bl&ots=un-zA8wMgs&sig=EWSRAdzaFYlCBGc4NoGh8-TunIw&hl=en&sa=X&ei=qelmUsCeF6muyQGW-4DgAg&ved=0CHQQ6AEwCA#v=onepage&q=shut%20down%20poison%20pill%20queue&f=false )

This ensures that the consumer gets a chance to processes all pending messages and then call 'consumer.shutdown()' to cleanly shut down.
We make sure that the consumer has completed its shut down by joining its thread (line 19), and only then do we shut down the producer
(line 20.)



## Current Issues

I have been struggling to find a Maven pom.xml recipe that will allow me to pull in an official version of 
Kafka from a public Maven repository.  Kafka is a very recent project so many of the currently available on-line 
examples (as of this writing -- October of 2013) don't seem to build correctly out of the box (at least for me.)  By contributing this project at least the 'run out of the box'requirement should be met.

Many examples depend on using maven install-file to get a Kafka jar that you build yourself from sources into your local 
repo ($HOME/.m2/repository).  A recent stack exchange article 
(see: http://stackoverflow.com/questions/17037209/where-can-i-find-maven-repository-for-kafka)
suggests an official Kafka .jar is available, but I haven't figured out the Maven incantations to have 
my build download this .jar. 

If someone could provide me with a patch for 'the right way' to do this with Maven I will update my project
accordingly.... Hopefully it will serve as a useful resource for other beginning Kafka developers.


For now, I have hacked my dependencies so that the version of Kafka I use is pulled from a work-in-progress
version of a storm-kafka integration project.  Well... it works for now, but I'm concerned the 'wip' versions
below will be deprecated. Then this project will loose its dependencies and fail to build properly. 
Also,  I really shouldn't be introducing storm for this simple Kafka example at this point in any case.



       <dependency>
            <groupId>storm</groupId>
            <artifactId>storm</artifactId>
            <version>0.9.0-wip17</version>
        </dependency>
        <dependency>
            <groupId>storm</groupId>
            <artifactId>storm-core</artifactId>
            <version>0.9.0-wip17</version>
        </dependency>
        <dependency>
            <groupId>storm</groupId>
            <artifactId>storm-kafka</artifactId>
            <version>0.9.0-wip16a-scala292</version>
        </dependency>
        <dependency>
