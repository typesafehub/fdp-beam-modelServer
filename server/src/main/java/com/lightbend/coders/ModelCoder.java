/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lightbend.coders;

import com.lightbend.model.Model;
import com.lightbend.model.ModelFactory;
import com.lightbend.model.Modeldescriptor;
import com.lightbend.model.PMML.PMMLModelFactory;
import com.lightbend.model.tensorflow.TensorflowModel;
import com.lightbend.model.tensorflow.TensorflowModelFactory;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.ExposedByteArrayOutputStream;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link ModelCoder} encodes a {@link TensorflowModel}
 */
public class ModelCoder extends AtomicCoder<Model> {

  private static final Map<Integer, ModelFactory> factories = new HashMap<Integer, ModelFactory>() {
    {
      put(Modeldescriptor.ModelDescriptor.ModelType.TENSORFLOW.getNumber(), TensorflowModelFactory.getInstance());
      put(Modeldescriptor.ModelDescriptor.ModelType.PMML.getNumber(), PMMLModelFactory.getInstance());
    }
  };
  public static ModelCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final ModelCoder INSTANCE = new ModelCoder();

  private static final TypeDescriptor<Model> TYPE_DESCRIPTOR = new TypeDescriptor<Model>() {};

  private static void writeModel(Model value, DataOutputStream dos)
          throws IOException {
    byte[] bytes = value.getBytes();
    VarInt.encode((long) bytes.length, dos);
    dos.write(bytes);
    VarInt.encode(value.getType(), dos);
  }

  private static Model readModel(DataInputStream dis) throws IOException {
    int len = (int)VarInt.decodeLong(dis);
    if (len < 0) {
      throw new CoderException("Invalid encoded value length: " + len);
    }
    byte[] bytes = new byte[len];
    dis.readFully(bytes);
    int type = (int)VarInt.decodeLong(dis);
    ModelFactory factory = factories.get(type);
    if (factory == null) {
      System.out.println("Unknown model type " + type);
      return null;
    }
    return factory.restore(bytes);
  }

  private ModelCoder() {}

  @Override
  public void encode(Model value, OutputStream outStream)
          throws IOException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(Model value, OutputStream outStream, Context context) throws IOException {
    if (value == null)
      throw new CoderException("cannot encode a null model");
    if (context.isWholeStream) {
      byte[] bytes = value.getBytes();
      byte[] types = ByteUtils.longToBytes(value.getType());
      if (outStream instanceof ExposedByteArrayOutputStream) {
         ((ExposedByteArrayOutputStream) outStream).writeAndOwn(bytes);
         ((ExposedByteArrayOutputStream) outStream).writeAndOwn(types);
      } else {
        outStream.write(bytes);
        outStream.write(types);
      }
    } else {
      writeModel(value, new DataOutputStream(outStream));
    }
  }

  @Override
  public Model decode(InputStream inStream) throws IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public Model decode(InputStream inStream, Context context) throws IOException {
    if (context.isWholeStream) {
      byte[] bytes = StreamUtils.getBytes(inStream);
      int type = (int)ByteUtils.bytesToLong(StreamUtils.getBytes(inStream));
      ModelFactory factory = factories.get(type);
      if (factory == null) {
        System.out.println("Unknown model type " + type);
        return null;
      }
      return factory.restore(bytes);
    } else {
      try {
        return readModel(new DataInputStream(inStream));
      } catch (EOFException | UTFDataFormatException exn) {
        // These exceptions correspond to decoding problems, so change
        // what kind of exception they're branded as.
        throw new CoderException(exn);
      }
    }
  }

  @Override
  public void verifyDeterministic() {}

  /**
   * {@inheritDoc}
   *
   * @return {@code true}. This coder is injective.
   */
  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  @Override
  public TypeDescriptor<Model> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  /**
   * {@inheritDoc}
   *
   * @return the byte size of the UTF-8 encoding of the a string or, in a nested context,
   * the byte size of the encoding plus the encoded length prefix.
   */
  @Override
  public long getEncodedElementByteSize(Model value)
          throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null Model");
    }
    int size = value.getBytes().length;
    return VarInt.getLength((long) size) + VarInt.getLength(value.getType()) + size;
  }

  public static class ByteUtils {
    private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

    public static byte[] longToBytes(long x) {
      buffer.putLong(0, x);
      return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
      buffer.put(bytes, 0, bytes.length);
      buffer.flip();//need flip
      return buffer.getLong();
    }
  }
}