package com.lightbend.kafka;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by boris on 5/17/17.
 */
public class JobConfiguration {
    private JobConfiguration(){}

    /**
     * Helper method to set the Kafka props from the pipeline options.
     *
     * @param options KafkaOptions
     * @return Kafka props
     */
    public static Map<String, Object> getKafkaSenderProps(KafkaOptions options) {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBroker());
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.RETRIES_CONFIG, "2");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "1024");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "1024000");

        return props;
    }

    /**
     * Helper method to set the Kafka props from the pipeline options.
     *
     * @param options KafkaOptions
     * @return Kafka props
     */
    public static Map<String, Object> getKafkaConsumerProps(KafkaOptions options, boolean data) {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBroker());
        if(data)
            props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getDataGroup());
        else
            props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getModelsGroup());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        return props;
    }

    public static Properties getKafkaConsumerProp(KafkaOptions options, boolean data) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBroker());
        if(data)
            props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getDataGroup());
        else
            props.put(ConsumerConfig.GROUP_ID_CONFIG, options.getModelsGroup());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        return props;
    }

    /**
     * Initializes some options for the Flink runner.
     *
     * @param args The command line args
     * @return the pipeline
     */
    public static Pipeline initializePipeline(String[] args) {
        KafkaOptions options = PipelineOptionsFactory.fromArgs(args).as(KafkaOptions.class);
        // Use Flink runner
        options.setRunner(FlinkRunner.class);
        // Set job name
        options.setJobName("BeamModelServer" );
        // Define this job as streaming
        options.setStreaming(true);

        // Runner configuration
        options.setCheckpointingInterval(1000L);
        options.setNumberOfExecutionRetries(5);
        options.setExecutionRetryDelay(3000L);

        return Pipeline.create(options);
    }
}
