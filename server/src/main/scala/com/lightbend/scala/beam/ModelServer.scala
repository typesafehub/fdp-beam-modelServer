package com.lightbend.scala.beam

import java.util

import com.lightbend.scala.beam.processors.{ConvertDataRecordFn, ConvertModelRecordFn, ScoringFn, SimplePrinterFn}
import com.lightbend.kafka.JobConfiguration
import com.lightbend.modelServer.model.ModelWithData
import com.lightbend.scala.beam.coders.ScalaDoubleCoder
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.{ByteArrayCoder, KvCoder, NullableCoder}
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.transforms.{Flatten, MapElements, ParDo}
import org.apache.beam.sdk.values.{KV, PCollectionList}

object ModelServer {

  def main(args: Array[String]): Unit = {
    // Create and initialize pipeline
    val options = JobConfiguration.initializePipeline(args)
    val p = Pipeline.create(options)

    // Coder to use for Kafka data - raw byte message
    val kafkaDataCoder = KvCoder.of(NullableCoder.of(ByteArrayCoder.of), ByteArrayCoder.of)

    // Data Stream - gets data records from Kafka topic
    // It has to use KV to work with state, which is always KV
    val dataStream = p
        .apply("data", KafkaIO.readBytes
          .withBootstrapServers(options.getBroker)
          .withTopics(util.Arrays.asList(options.getKafkaDataTopic))
          .updateConsumerProperties(JobConfiguration.getKafkaConsumerProps(options, true))
          .withoutMetadata).setCoder(kafkaDataCoder)
        // Convert Kafka message to WineRecord
        .apply("Parse data records", ParDo.of(new ConvertDataRecordFn))

    // Models Stream - get model records from Kafka
    // It has to use KV to work with state, which is always KV
    val modelStream = p
          .apply("models", KafkaIO.readBytes
            .withBootstrapServers(options.getBroker)
            .withTopics(util.Arrays.asList(options.getKafkaModelsTopic))
            .updateConsumerProperties(JobConfiguration.getKafkaConsumerProps(options, false))
            .withoutMetadata).setCoder(kafkaDataCoder)
          // Convert Kafka record to ModelDescriptor
          .apply("Parse model", ParDo.of(new ConvertModelRecordFn))

    // Create a combined PCollection stream combining both model and data streams.
    val combinedStream = PCollectionList.of(dataStream).and(modelStream)
      // Flatten the list
      .apply(Flatten.pCollections[KV[String, ModelWithData]])

    // Score models and print result
    val scoringResults = combinedStream
      // Score data using current model
      .apply("Scoring the model", ParDo.of(new ScoringFn)).setCoder(ScalaDoubleCoder.of)
      // Print scoring result
      .apply("Print result", MapElements.via(new SimplePrinterFn[Double]))

    // Run the pipeline
    p.run
  }
}