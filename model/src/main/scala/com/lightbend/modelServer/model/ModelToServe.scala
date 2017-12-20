package com.lightbend.modelServer.model

import java.io._

import com.lightbend.model.modeldescriptor.ModelDescriptor
import com.lightbend.model.winerecord.WineRecord

import scala.collection.Map
import com.lightbend.modelServer.model.PMML.PMMLModel
import com.lightbend.modelServer.model.tensorflow.TensorFlowModel

import scala.util.Try

/**
 * Created by boris on 5/8/17.
 */
object ModelToServe {
  private val factories = Map(
    ModelDescriptor.ModelType.PMML.name -> PMMLModel,
    ModelDescriptor.ModelType.TENSORFLOW.name -> TensorFlowModel
  )

  private val factoriesInt = Map(
    ModelDescriptor.ModelType.PMML.index -> PMMLModel,
    ModelDescriptor.ModelType.TENSORFLOW.index -> TensorFlowModel
  )

  def fromByteArray(message: Array[Byte]): Try[ModelToServe] = Try {
    val m = ModelDescriptor.parseFrom(message)
    m.messageContent.isData match {
      case true => new ModelToServe(m.name, m.description, m.modeltype, m.getData.toByteArray, m.dataType)
      case _ => throw new Exception("Location based is not yet supported")
    }
  }

  def fromModelToServe(descriptor : ModelToServe): Try[Model] = Try{
    println(s"New model - $descriptor")
    factories.get(descriptor.modelType.name) match {
      case Some(factory) => factory.create(descriptor)
      case _ => throw new Throwable("Undefined model type")
    }
  }

  def factoryOrdinal(ordinal : Int) : Option[ModelFactory] = factoriesInt.get(ordinal)
}

case class ModelToServe(name: String, description: String,
  modelType: ModelDescriptor.ModelType, model: Array[Byte], dataType: String) {}

case class ModelWithData(model: Option[ModelToServe], data : Option[WineRecord])
