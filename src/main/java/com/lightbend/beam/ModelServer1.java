package com.lightbend.beam;

import com.lightbend.kafka.JobConfiguration;
import com.lightbend.kafka.KafkaOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.Arrays;

/**
 * Created by boris on 5/17/17.
 * Loosely based on
 *  https://beam.apache.org/blog/2017/02/13/stateful-processing.html
 */
public class ModelServer1 {

    public static void main(String[] args) {

        // Create and initialize pipeline
        KafkaOptions options = JobConfiguration.initializePipeline(args);
        Pipeline p = Pipeline.create(options);

        // Coder to use for Kafka data - raw byte message
        KvCoder<byte[], byte[]> kafkaDataCoder = KvCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of());

        // Data Stream - gets data records from Kafka topic
        // It has to use KV to work with state, which is always KV
        PCollection<KV<String, ModelServer1Support.DataWithModel>> dataStream = p
                .apply("data", KafkaIO.readBytes()
                        .withBootstrapServers(options.getBroker())
                        .withTopics(Arrays.asList(options.getKafkaDataTopic()))
                        .updateConsumerProperties(JobConfiguration.getKafkaConsumerProps(options, true))
                        .withoutMetadata()).setCoder(kafkaDataCoder)
                // Convert Kafka message to WineRecord
                .apply("Parse data records", ParDo.of(new ModelServer1Support.ConvertDataRecordFunction()));

        // Models Stream - get model records from Kafka
        // It has to use KV to work with state, which is always KV
        PCollection<KV<String,ModelServer1Support.DataWithModel>> modelStream = p
                .apply("models", KafkaIO.readBytes()
                        .withBootstrapServers(options.getBroker())
                        .withTopics(Arrays.asList(options.getKafkaModelsTopic()))
                        .updateConsumerProperties(JobConfiguration.getKafkaConsumerProps(options, false))
                        .withoutMetadata()).setCoder(kafkaDataCoder)
                // Convert Kafka record to ModelDescriptor
                .apply("Parse model", ParDo.of(new ModelServer1Support.ConvertModelRecordFunction()));

        // Create a combined PCollection stream combining both model and data streams.
        PCollection<KV<String,ModelServer1Support.DataWithModel>> combinedStream = PCollectionList.of(dataStream).and(modelStream)
                // Flatten the list
                .apply(Flatten.pCollections());
        
        // Score models and print result
        PCollection<Double> scoringResults = combinedStream
                // Score data using current model
                .apply("Scoring the model", ParDo.of(new ModelServer1Support.ScoringdFunction()))
                // Print scoring result
                .apply("Print result", MapElements.via(new ModelServer1Support.SimplePrinterFn<>()));

        // Run the pipeline
        p.run();
    }
}