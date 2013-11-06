# Kafka Spout Example



Example Illustrating a Kafka Consumer Spout, a Kafka Producer Bolt, and an Esper Streaming Query Bolt

## Description


The test class 'ExternalFeedRoutedToEsperAndThenToKakfaOutputBoltTest' illustrates how to wire up Kafka, Storm and Esper.  In this test, ExternalFeedToKafkaAdapterSpout  pushes messages into a topic. These messages are then routed into an EsperBolt which uses the Esper query language to do some simple filtering, We then route the filtered messages to a KafkaOutputBolt which dumps the filtered messages on a second topic. We use an instance of Kafka MessageConsumer to pull those messages off the second topic, and we verify that what we got is equal to what we expect.

We use Thomas Dudziak's storm-esper library to bind an Esper query processing engine instance to a Storm Bolt  (More info on that library is available here: http://tomdzk.wordpress.com/2011/09/28/storm-esper)



A list of the main components involved in this example follows:

KafkaOutputBolt 

      A first pass implementation of a generic Kafka Output Bolt that takes whatever tuple it
      recieves, JSON-ifies it, and dumps it on the Kafka topic that is configured in the 
      constructor. 

ExternalFeedToKafkaAdapterSpout

      Accepts an IFeedItemProvider instance (running on a separte thread spawned by this 
      adapter spout) that is responsible for acquiring data from  an external source, which 
      is then transferred to the adapter spout to be deposited on a Kafka topic (the name 
      of which is set as an argument to the adapter spout constructor.)
     


Testing Support:

AbstractStormWithKafkaTest 

      Simplifies testing of Storm components that consume or produce data items from or to Kafka.
      Operates via  a 'template method' series of steps, wherein the BeforeClass method sets up a 
      Storm Local cluster, then waits for the zookeeper instance started by that cluster to 'boot up', 
      then starts an in-process Kafka server using that zookeeper, and then creates a topic whose
      name is derived from the name of the base class test.
     
      Subclasses only need to implement the abstract createTopology() method (and perhaps
      override 'verifyResults())' which is currently kind of hard coded to our first two subclasses of
      this base class.
     





## Building and Running

After downloading the project, cd to the directory in which  this README is located, then issue the 2 commands below

     mvn clean  compile test
    



