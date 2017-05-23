package com.lightbend.kafka;

/**
 * Created by boris on 5/17/17.
 */

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/**
 * Custom (Kafka related) options for the Pipeline.
 */
public interface KafkaOptions extends FlinkPipelineOptions {

    @Description("The Kafka data topic to read data from")
    @Default.String(ApplicationKafkaParameters.DATA_TOPIC)
    String getKafkaDataTopic();

    void setKafkaDataTopic(String value);

    @Description("The Kafka topic to read models from")
    @Default.String(ApplicationKafkaParameters.MODELS_TOPIC)
    String getKafkaModelsTopic();

    void setKafkaModelsTopic(String value);

    @Description("The Kafka Broker to read from")
    @Default.String(ApplicationKafkaParameters.LOCAL_KAFKA_BROKER)
    String getBroker();

    void setBroker(String value);

    @Description("The Zookeeper server to connect to")
    @Default.String(ApplicationKafkaParameters.LOCAL_ZOOKEEPER_HOST)
    String getZookeeper();

    void setZookeeper(String value);

    @Description("The Data Reading groupId")
    @Default.String(ApplicationKafkaParameters.DATA_GROUP)
    String getDataGroup();

    void setDataGroup(String value);

    @Description("The Models Reading groupId")
    @Default.String(ApplicationKafkaParameters.MODELS_GROUP)
    String getModelsGroup();

    void setModelsGroup(String value);
}