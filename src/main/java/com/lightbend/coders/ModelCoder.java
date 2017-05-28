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
import com.lightbend.model.TensorModel;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link ModelCoder} encodes a {@link TensorModel}
 */
public class ModelCoder extends CustomCoder<Model> {

  public static ModelCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final ModelCoder INSTANCE = new ModelCoder();
  private static final TypeDescriptor<Model> TYPE_DESCRIPTOR = new TypeDescriptor<Model>() {};
  private static final TensorModelCoder tensorCoder = TensorModelCoder.of();
  private static final SerializableCoder<Model> serializableCoder = SerializableCoder.of(Model.class);

  private ModelCoder() {}

  @Override
  public void encode(Model value, OutputStream outStream)
          throws IOException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(Model value, OutputStream outStream, Context context)
          throws IOException {
    if (value instanceof TensorModel)
      tensorCoder.encode((TensorModel)value,outStream, context);
    else
      serializableCoder.encode(value,outStream,context);
  }

  @Override
  public Model decode(InputStream inStream) throws IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public Model decode(InputStream inStream, Context context)
          throws IOException {
    try{
        // Try serializable first
        return serializableCoder.decode(inStream,context);
    }
    catch (Throwable t){
        // Use Tensor otherwise
        return tensorCoder.decode(inStream, context);
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
}