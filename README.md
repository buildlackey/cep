

This project includes:

    unit tests and sample programs that illustrate how to 
    develop complex event processing (CEP) applications on top of Storm, Kafka 
    and Esper.

    a Wiki containing notes on best practices and guidelines for using 
    the above frameworks for CEP development.



Subdirectories:




kafka

    includes

        * A Simple Kafka Produce/Consumer Example With In-Memory Kafka and Zookeeper Test Fixture Servers

            (for Kafka 0.7.x)


kafka-0.8.x

    includes

        * A Simple Kafka Produce/Consumer Example With In-Memory Kafka and Zookeeper Test Fixture Servers

            (for Kafka 0.8.x)

storm+kafka

    includes

        * An Integration test that pushes messages into Kafka and retrieves those messages with a Kafka/Storm spout.



esper+storm+kafka

    includes


        * An Integration test that pushes messages into Kafka, pulls them back (via Kafka Spout), filters them using Esper, and dumps them back out to another Kafka Spout (via KafkaOutputBolt).


