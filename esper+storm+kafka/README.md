# Kafka Spout Example



Example Illustrating a Kafka Consumer Spout, a Kafka Producer Bolt, and an Esper Streaming Query Bolt

## Description

This example illustrates how to wire up Kafka, Storm and Esper. Not quite done yet, but we have developed some useful components.


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
(note that the second command has two variants):

     mvn clean  compile test
    
## Implementation Details



