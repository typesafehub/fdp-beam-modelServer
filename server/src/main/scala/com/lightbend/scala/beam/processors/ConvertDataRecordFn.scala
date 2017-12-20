package com.lightbend.scala.beam.processors

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import com.lightbend.modelServer.model.{DataRecord, ModelWithData}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

import scala.util.Success

class ConvertDataRecordFn extends DoFn[KV[Array[Byte], Array[Byte]], KV[String, ModelWithData]]{

  @ProcessElement
  def processElement(ctx: DoFn[KV[Array[Byte], Array[Byte]], KV[String, ModelWithData]]#ProcessContext) : Unit = {
    // Unmarshall record
    DataRecord.fromByteArray(ctx.element.getValue) match {
      case Success(record) => ctx.output(KV.of(record.dataType, ModelWithData(None, Some(record))))
      case _ => println("Exception parsing data record")
    }
  }
}