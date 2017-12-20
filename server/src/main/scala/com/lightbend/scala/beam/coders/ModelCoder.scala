package com.lightbend.scala.beam.coders

import java.io._

import org.apache.beam.sdk.coders.{AtomicCoder, CoderException}
import com.lightbend.modelServer.model.{Model, ModelToServe}
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.util.VarInt

class ModelCoder extends AtomicCoder[Model] {

  import ModelCoder._

  override def encode(value: Model, outStream: OutputStream) = {
    if (value == null) throw new CoderException("cannot encode a null model")
    writeModel(value, new DataOutputStream(outStream))
  }

  override def decode(inStream: InputStream) = readModel(new DataInputStream(inStream))

  override def verifyDeterministic(): Unit = {}

  override def consistentWithEquals : Boolean = true

  override def getEncodedTypeDescriptor: TypeDescriptor[Model] = TYPE_DESCRIPTOR
  @throws[Exception]
  override def getEncodedElementByteSize(value: Model): Long = {
    if (value == null) throw new CoderException("cannot encode a null Model")
    val size = value.toBytes().length
    VarInt.getLength(size.toLong) + VarInt.getLength(value.getType) + size
  }
}

object ModelCoder{

  private val INSTANCE = new ModelCoder()
  private val TYPE_DESCRIPTOR = new TypeDescriptor[Model]() {}

  def of: ModelCoder = INSTANCE

  @throws[IOException]
  private def writeModel(value: Model, dos: DataOutputStream): Unit = {
    val bytes = value.toBytes()
    VarInt.encode(bytes.length.toLong, dos)
    dos.write(bytes)
    VarInt.encode(value.getType, dos)
  }

  @throws[IOException]
  private def readModel(dis: DataInputStream): Model = {
    VarInt.decodeLong(dis).toInt match {
      case len if len <= 0 => throw new CoderException("Invalid encoded value length: " + len)
      case len => {
        val bytes = new Array[Byte](len)
        dis.readFully(bytes)
        val modelType = VarInt.decodeLong(dis).toInt
        ModelToServe.factoryOrdinal(modelType) match {
          case Some(factory) => factory.restore(bytes)
          case _ => throw new IOException(s"Unknown model type $modelType")
        }
      }
    }
  }
}