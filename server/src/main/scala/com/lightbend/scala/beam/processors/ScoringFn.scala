package com.lightbend.scala.beam.processors

import com.lightbend.modelServer.model.{Model, ModelToServe, ModelWithData}
import com.lightbend.scala.beam.coders.ModelCoder
import org.apache.beam.sdk.state.{StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StateId}

import scala.util.Success


class ScoringFn extends DoFn[KV[String, ModelWithData],Double] {

  @StateId("model") private val modelSpec = StateSpecs.value(ModelCoder.of)

  @ProcessElement
  def processElement(ctx: DoFn[KV[String, ModelWithData],Double]#ProcessContext, @StateId("model") modelState: ValueState[Model]): Unit = {

    // Get current element
    val input = ctx.element
    // Check if we got the model
    input.getValue.model match{
      case Some(descriptor) => {
        ModelToServe.fromModelToServe(descriptor) match {
          case Success(newModel) => {
            val current = modelState.read()
            if (current != null)
              current.cleanup()
            modelState.write(newModel)
          }
          case _ =>
        }
      }
      case _ => // Skip
    }
    input.getValue.data match{
      case Some(record) => {
        val model = modelState.read()
        if (model == null) { // No model currently
          System.out.println("No model available - skipping")
        }
        else { // Score the model
          val start = System.currentTimeMillis
          val quality = model.score(record.asInstanceOf[AnyVal]).asInstanceOf[Double]
          val duration = System.currentTimeMillis - start
          System.out.println("Calculated quality - " + quality + " in " + duration + "ms")
          // Propagate result
          ctx.output(quality)
        }
      }
      case _ => // Skip
    }
  }
}