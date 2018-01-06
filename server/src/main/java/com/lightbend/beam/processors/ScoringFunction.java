package com.lightbend.beam.processors;

import com.lightbend.coders.ModelCoder;
import com.lightbend.model.DataWithModel;
import com.lightbend.model.Model;
import com.lightbend.model.ModelToServe;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.Optional;


// Based on https://beam.apache.org/blog/2017/02/13/stateful-processing.html
public class ScoringFunction extends DoFn<KV<String,DataWithModel>, Double> {

    // Internal state
    @StateId("model")
    private final StateSpec<ValueState<Model>> modelSpec = StateSpecs.value(ModelCoder.of());

    @ProcessElement
    public void processElement(DoFn<KV<String,DataWithModel>, Double>.ProcessContext ctx, @StateId("model") ValueState<Model> modelState) {
        // Get current element
        KV<String, DataWithModel> input = ctx.element();
        // Check if we got the model
        ModelToServe descriptor = input.getValue().getModel();
        // Get current model
        Model model = modelState.read();
        if (descriptor != null) {
            // Process model - store it
            System.out.println("New scoring model " + descriptor);
            Optional<Model> current = ModelToServe.create(descriptor);
            if (current.isPresent()) {
                if (model != null)
                    model.cleanup();
                // Create and store the model
                modelState.write(current.get());
            } else
                System.out.println("Error converting model Descriptor" + descriptor);
        }
        // Process data
        else {
            if (model == null)
                // No model currently
                System.out.println("No model available - skipping");
            else {
                // Score the model
                long start = System.currentTimeMillis();
                double quality = (double) model.score(input.getValue().getData());
                long duration = System.currentTimeMillis() - start;
                System.out.println("Calculated quality - " + quality + " in " + duration + "ms");
                // Propagate result
                ctx.output(quality);
            }
        }
    }
}