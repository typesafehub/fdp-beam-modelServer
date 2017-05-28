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

import com.lightbend.model.TensorModel;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.ExposedByteArrayOutputStream;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.*;

/**
 * A {@link TensorModelCoder} encodes a {@link TensorModel} 
 */
public class TensorModelCoder extends AtomicCoder<TensorModel> {

  public static TensorModelCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final TensorModelCoder INSTANCE = new TensorModelCoder();
  private static final TypeDescriptor<TensorModel> TYPE_DESCRIPTOR = new TypeDescriptor<TensorModel>() {};

  private static void writeTensorModel(TensorModel value, DataOutputStream dos)
          throws IOException {
    byte[] bytes = value.getGraph().toGraphDef();
    VarInt.encode(bytes.length, dos);
    dos.write(bytes);
  }

  private static TensorModel readTensorModel(DataInputStream dis) throws IOException {
    int len = VarInt.decodeInt(dis);
    if (len < 0) {
      throw new CoderException("Invalid encoded string length: " + len);
    }
    byte[] bytes = new byte[len];
    dis.readFully(bytes);
    return new TensorModel(bytes);
  }

  private TensorModelCoder() {}

  @Override
  public void encode(TensorModel value, OutputStream outStream)
          throws IOException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(TensorModel value, OutputStream outStream, Context context)
          throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null String");
    }
    if (context.isWholeStream) {
      byte[] bytes = value.getGraph().toGraphDef();
      if (outStream instanceof ExposedByteArrayOutputStream) {
        ((ExposedByteArrayOutputStream) outStream).writeAndOwn(bytes);
      } else {
        outStream.write(bytes);
      }
    } else {
      writeTensorModel(value, new DataOutputStream(outStream));
    }
  }

  @Override
  public TensorModel decode(InputStream inStream) throws IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public TensorModel decode(InputStream inStream, Context context)
          throws IOException {
    if (context.isWholeStream) {
      byte[] bytes = StreamUtils.getBytes(inStream);
      return new TensorModel(bytes);
    } else {
      try {
        return readTensorModel(new DataInputStream(inStream));
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
  public TypeDescriptor<TensorModel> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  /**
   * {@inheritDoc}
   *
   * @return the byte size of the UTF-8 encoding of the a string or, in a nested context,
   * the byte size of the encoding plus the encoded length prefix.
   */
  @Override
  public long getEncodedElementByteSize(TensorModel value)
          throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null String");
    }
    int size = value.getGraph().toGraphDef().length;
    return VarInt.getLength(size) + size;
  }
}