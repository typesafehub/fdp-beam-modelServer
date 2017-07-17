package com.lightbend.beam.obsolete;

import com.lightbend.model.Winerecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Created by boris on 5/18/17.
 */

public class DataRecordProcessor {

    public static class ConvertDataRecordToKVFunction extends DoFn<KV<byte[], byte[]>, KV<String,Winerecord.WineRecord>> {

        @ProcessElement
        public void processElement(ProcessContext c){
            KV<byte[],byte[]> input = c.element();
            try {
                // Unmarshall record
                Winerecord.WineRecord record = Winerecord.WineRecord.parseFrom(input.getValue());
                // Return it
                c.output(KV.of(record.getDataType(), record));
            } catch (Throwable t) {
                // Oops
                System.out.println("Exception parsing input record" + new String(input.getValue()));
                t.printStackTrace();
            }
        }
    }


    public static class PrinterSimpleFn<T> extends SimpleFunction<T, T>{
        @Override
        public T apply(T input) {
            System.out.println("Processing data " + input);
            return input;
        }
    }

    public static class PrinterKVFn<K,V> extends SimpleFunction<KV<K,V>, KV<K,V>>{
        @Override
        public KV<K,V> apply(KV<K,V> input) {
            System.out.println("Processing data with key " + input.getKey() + " and value " + input.getValue());
            return input;
        }
    }
}
