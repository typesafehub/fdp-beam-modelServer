package com.lightbend.scala.beam.kafka

import java.util
import org.apache.beam.runners.flink.FlinkRunner
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

object JobConfiguration {
  /**
    * Helper method to set the Kafka props from the pipeline options.
    *
    * @param options KafkaOptions
    * @return Kafka props
    */
    def getKafkaSenderProps(options: KafkaOptions): util.Map[String, AnyRef] = {
      val props: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBroker)
      props.put(ProducerConfig.ACKS_CONFIG, "all")
      props.put(ProducerConfig.RETRIES_CONFIG, "2")
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1024")
      props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
      props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "1024000")
      props
    }

  def getKafkaConsumerProps(options: KafkaOptions, data: Boolean): util.Map[String, AnyRef] = {
    val props: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBroker)
    if (data) props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getDataGroup)
    else props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getModelsGroup)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
    props
  }

  /**
    * Initializes some options for the Flink runner.
    *
    * @param args The command line args
    * @return the pipeline
    */
  def initializePipeline(args: Array[String]): KafkaOptions = {
    val options: KafkaOptions = PipelineOptionsFactory.fromArgs(args:_*).as(classOf[KafkaOptions])
    // Use Flink runner
    options.setRunner(classOf[FlinkRunner])
    // Set job name
    options.setJobName("BeamModelServer")
    // Define this job as streaming
    options.setStreaming(true)
    // Runner configuration
    options.setCheckpointingInterval(1000L)
    options.setNumberOfExecutionRetries(5)
    options.setExecutionRetryDelay(3000L)
    options
  }
}