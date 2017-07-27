package com.lightbend.beam.processors;

import com.lightbend.model.ModelToServe;
import com.lightbend.model.DataWithModel;
import com.lightbend.model.Modeldescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

// Converting Byte array to model descriptor
public class ConvertModelRecordFunction extends DoFn<KV<byte[], byte[]>, KV<String,DataWithModel>> {

    @ProcessElement
    public void processElement(DoFn<KV<byte[], byte[]>, KV<String, DataWithModel>>.ProcessContext ctx){
        // Get current element
        KV<byte[],byte[]> input = ctx.element();
        try {
            // Unmarshall record
            Modeldescriptor.ModelDescriptor model = Modeldescriptor.ModelDescriptor.parseFrom(input.getValue());
            // Return it
            if(model.getMessageContentCase().equals(Modeldescriptor.ModelDescriptor.MessageContentCase.DATA)){
                ctx.output(KV.of(model.getDataType(), new DataWithModel(new ModelToServe(
                        model.getName(), model.getDescription(), model.getModeltype(),
                        model.getData().toByteArray(), null, model.getDataType()))));
            }
            else
                System.out.println("Location based model is not yet supported");
        } catch (Throwable t) {
            // Oops
            System.out.println("Exception parsing input record" + new String(input.getValue()));
            t.printStackTrace();
        }
    }
}
