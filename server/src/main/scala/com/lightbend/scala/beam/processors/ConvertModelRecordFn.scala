package com.lightbend.scala.beam.processors

import com.lightbend.modelServer.model.{ModelToServe, ModelWithData}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.values.KV

import scala.util.Success

class ConvertModelRecordFn extends DoFn[KV[Array[Byte], Array[Byte]], KV[String, ModelWithData]]{

  @ProcessElement
  def processElement(ctx: DoFn[KV[Array[Byte], Array[Byte]], KV[String, ModelWithData]]#ProcessContext) : Unit = {
    // Unmarshall record
    ModelToServe.fromByteArray(ctx.element.getValue) match {
      case Success(model) => ctx.output(KV.of(model.dataType, ModelWithData(Some(model), None)))
      case _ =>  println("Exception parsing input model record")
    }
  }
}
