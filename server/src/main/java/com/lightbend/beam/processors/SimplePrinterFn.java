package com.lightbend.beam.processors;

import org.apache.beam.sdk.transforms.SimpleFunction;

// Simple function to print content of collection
public class SimplePrinterFn<T> extends SimpleFunction<T, T> {
    @Override
    public T apply(T input) {
        // Print the variable
        System.out.println("Processing data " + input);
        // Propagate it
        return input;
    }
}
