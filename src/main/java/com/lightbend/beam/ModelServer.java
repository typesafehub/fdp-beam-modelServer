package com.lightbend.beam;

import com.lightbend.kafka.JobConfiguration;
import com.lightbend.kafka.KafkaOptions;
import com.lightbend.model.Model;
import com.lightbend.model.Winerecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by boris on 5/17/17.
 * Loosely based on
 *  https://beam.apache.org/blog/2017/02/13/stateful-processing.html
 */
public class ModelServer {

    public static void main(String[] args) {

        // Create and initialize pipeline
        Pipeline p = JobConfiguration.initializePipeline(args);
        KafkaOptions options = p.getOptions().as(KafkaOptions.class);

        KvCoder<byte[], byte[]> kafkaDataCoder = KvCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of());
        SerializableCoder<Model> modelsCoder = SerializableCoder.of(Model.class);

        // Data Stream
/*        PCollection<Winerecord.WineRecord> dataStream = p
                .apply("data", KafkaIO.readBytes()
                        .withBootstrapServers(options.getBroker())
                        .withTopics(Arrays.asList(options.getKafkaDataTopic()))
                        .updateConsumerProperties(JobConfiguration.getKafkaConsumerProps(options, true))
                        .withoutMetadata()).setCoder(kafkaDataCoder)
                // Group into fixed-size windows
                .apply(Window.<KV<byte[], byte[]>>into(FixedWindows.of(
                        Duration.standardSeconds(1)))
                        .triggering(AfterWatermark.pastEndOfWindow()).
                                withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply("Parse data records", ParDo.of(new DataRecordProcessor.ConvertDataRecordFunction()));*/

        PCollection<KV<String,Iterable<Winerecord.WineRecord>>> dataStream = p
                .apply("data", KafkaIO.readBytes()
                        .withBootstrapServers(options.getBroker())
                        .withTopics(Arrays.asList(options.getKafkaDataTopic()))
                        .updateConsumerProperties(JobConfiguration.getKafkaConsumerProps(options, true))
                        .withoutMetadata()).setCoder(kafkaDataCoder)
                // Group into fixed-size windows
                .apply(Window.<KV<byte[], byte[]>>into(FixedWindows.of(
                        Duration.standardSeconds(1)))
                        .triggering(AfterWatermark.pastEndOfWindow()).
                                withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply("Parse data records", ParDo.of(new DataRecordProcessor.ConvertDataRecordToKVFunction()))
                .apply("GroupByKey",GroupByKey.<String, Winerecord.WineRecord>create());

        // Models Stream
        PCollection<ModelRecordProcessor.ModelToServe> modelStream = p
                .apply("models", KafkaIO.readBytes()
                        .withBootstrapServers(options.getBroker())
                        .withTopics(Arrays.asList(options.getKafkaModelsTopic()))
                        .updateConsumerProperties(JobConfiguration.getKafkaConsumerProps(options, false))
                        .withoutMetadata()).setCoder(kafkaDataCoder)
                // Group into fixed-size windows
                .apply(Window.<KV<byte[], byte[]>>into(FixedWindows.of(
                        Duration.standardSeconds(1)))
                        .triggering(AfterWatermark.pastEndOfWindow()).
                                withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply("Parse model", ParDo.of(new ModelRecordProcessor.ConvertModelRecordFunction()))
                .apply("Print ", MapElements.via(new DataRecordProcessor.PrinterSimpleFn<ModelRecordProcessor.ModelToServe>()));

        // Build a model view to use as a side input
        PCollectionView<Model> models = modelStream
                .apply("building model state",
                        Combine.globally(new ModelRecordProcessor.ModelFromEventsFn()).withoutDefaults().asSingletonView());

        PCollection<Double> scoringResults = dataStream
                .apply("Scoring the model", ParDo.withSideInputs(models).of(
                        new DoFn<KV<String,Iterable<Winerecord.WineRecord>>, Double>(){

                    @ProcessElement
                    public void processElement(ProcessContext ctx){
                        KV<String,Iterable<Winerecord.WineRecord>> record = ctx.element();
                        Model model = ctx.sideInput(models);
                        if(model == null)
                            System.out.println("No model available - skipping");
                        else{
                            // Ignore key for now
                            Iterator<Winerecord.WineRecord> iterator = record.getValue().iterator();
                            while(iterator.hasNext()) {
                                long start = System.currentTimeMillis();
                                double quality = (double) model.score(iterator.next());
                                long duration = System.currentTimeMillis() - start;
                                System.out.println("Calculated quality - " + quality + " in " + duration + "ms");
                                ctx.output(quality);
                            }
                        }
                    };
                }));
        p.run();
    }
}