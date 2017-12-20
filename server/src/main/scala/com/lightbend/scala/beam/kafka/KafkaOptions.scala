package com.lightbend.scala.beam.kafka

import com.lightbend.configuration.ApplicationKafkaParameters
import org.apache.beam.runners.flink.FlinkPipelineOptions
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description

trait KafkaOptions extends FlinkPipelineOptions {

  @Description("The Kafka data topic to read data from")
  @Default.String(ApplicationKafkaParameters.DATA_TOPIC) def getKafkaDataTopic: String

  def setKafkaDataTopic(value: String): Unit

  @Description("The Kafka topic to read models from")
  @Default.String(ApplicationKafkaParameters.MODELS_TOPIC) def getKafkaModelsTopic: String

  def setKafkaModelsTopic(value: String): Unit

  @Description("The Kafka Broker to read from")
  @Default.String(ApplicationKafkaParameters.LOCAL_KAFKA_BROKER) def getBroker: String

  def setBroker(value: String): Unit

  @Description("The Zookeeper server to connect to")
  @Default.String(ApplicationKafkaParameters.LOCAL_ZOOKEEPER_HOST) def getZookeeper: String

  def setZookeeper(value: String): Unit

  @Description("The Data Reading groupId")
  @Default.String(ApplicationKafkaParameters.DATA_GROUP) def getDataGroup: String

  def setDataGroup(value: String): Unit

  @Description("The Models Reading groupId")
  @Default.String(ApplicationKafkaParameters.MODELS_GROUP) def getModelsGroup: String

  def setModelsGroup(value: String): Unit
}