/*
 * Author: cbedford
 * Date: 11/12/13
 * Time: 4:58 PM
 */


import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import org.tomdz.storm.esper.EsperBolt;
import storm.kafka.*;

public class TopologyInitializer {
    public static int STORM_KAFKA_FROM_READ_FROM_CURRENT_OFFSET = -1;
    public static int STORM_KAFKA_FROM_READ_FROM_START = -2;

    public static StormTopology createTopology(String zookeeperConnectString,
                                               String kafkaBrokerConnectString,
                                               String inputTopic,
                                               String outputTopic,
                                               IFeedItemProvider feedItemProvider,
                                               boolean kafkaOutputBoltRawMode) {
        TopologyBuilder builder = new TopologyBuilder();
        IRichSpout feedSpout =
                new ExternalFeedToKafkaAdapterSpout(
                        feedItemProvider,
                        kafkaBrokerConnectString,
                        inputTopic, null);
        EsperBolt esperBolt = createEsperBolt();
        KafkaOutputBolt kafkaOutputBolt =
                new KafkaOutputBolt(kafkaBrokerConnectString, outputTopic, null, kafkaOutputBoltRawMode);

        builder.setSpout("externalFeedSpout", feedSpout);   // these spouts are bound together by shared topic
        builder.setSpout("kafkaSpout", createKafkaSpout(zookeeperConnectString, inputTopic));

        builder.setBolt("esperBolt", esperBolt, 1)
                .shuffleGrouping("kafkaSpout");
        builder.setBolt("kafkaOutputBolt", kafkaOutputBolt, 1)
                .shuffleGrouping("esperBolt");
        return builder.createTopology();
    }

    public static EsperBolt createEsperBolt() {
        String esperQuery=
                "select  str as found from OneWordMsg.win:length_batch(2) where str like '%at%'";
        EsperBolt esperBolt = new EsperBolt.Builder()
                .inputs().aliasComponent("kafkaSpout").
                        withFields("str").ofType(String.class).toEventType("OneWordMsg")
                .outputs().onDefaultStream().emit("found")
                .statements().add(esperQuery)
                .build();
        return esperBolt;
    }

    public static KafkaSpout createKafkaSpout(String zkConnect, String topicName) {
        BrokerHosts brokerHosts = new ZkHosts(zkConnect);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topicName, "", "storm");
        kafkaConfig.forceStartOffsetTime(STORM_KAFKA_FROM_READ_FROM_START);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(kafkaConfig);
    }
}
