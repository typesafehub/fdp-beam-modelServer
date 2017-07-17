package com.lightbend.beam.processors;

import com.lightbend.model.DataWithModel;
import com.lightbend.model.Winerecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

// Converting Byte array to data record

public class ConvertDataRecordFunction extends DoFn<KV<byte[], byte[]>, KV<String, DataWithModel>> {

    @ProcessElement
    public void processElement(DoFn<KV<byte[], byte[]>, KV<String, DataWithModel>>.ProcessContext ctx) {

        // Get current element
        KV<byte[], byte[]> input = ctx.element();
        try {
            // Unmarshall record
            Winerecord.WineRecord record = Winerecord.WineRecord.parseFrom(input.getValue());
            // Return it
            ctx.output(KV.of(record.getDataType(), new DataWithModel(record)));
        } catch (Throwable t) {
            // Oops
            System.out.println("Exception parsing input record" + new String(input.getValue()));
            t.printStackTrace();
        }
    }
}
