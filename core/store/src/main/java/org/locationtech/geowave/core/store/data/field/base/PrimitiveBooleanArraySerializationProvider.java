/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field.base;

import java.nio.ByteBuffer;
import java.util.BitSet;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldSerializationProviderSpi;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class PrimitiveBooleanArraySerializationProvider implements
    FieldSerializationProviderSpi<boolean[]> {
  @Override
  public FieldReader<boolean[]> getFieldReader() {
    return new PrimitiveBooleanArrayReader();
  }

  @Override
  public FieldWriter<boolean[]> getFieldWriter() {
    return new PrimitiveBooleanArrayWriter();
  }

  private static class PrimitiveBooleanArrayReader implements FieldReader<boolean[]> {

    @Override
    public boolean[] readField(final byte[] fieldData) {
      if ((fieldData == null) || (fieldData.length == 0)) {
        return null;
      }
      final ByteBuffer buff = ByteBuffer.wrap(fieldData);
      final int count = VarintUtils.readUnsignedInt(buff);
      final BitSet bits = BitSet.valueOf(buff);
      final boolean[] result = new boolean[count];
      for (int i = 0; i < bits.length(); i++) {
        result[i] = bits.get(i);
      }
      return result;
    }
  }

  private static class PrimitiveBooleanArrayWriter implements FieldWriter<boolean[]> {
    @Override
    public byte[] writeField(final boolean[] fieldValue) {
      if (fieldValue == null) {
        return new byte[] {};
      }
      final BitSet bits = new BitSet(fieldValue.length);
      for (int i = 0; i < fieldValue.length; i++) {
        bits.set(i, fieldValue[i]);
      }
      final byte[] bytes = bits.toByteArray();
      int size = VarintUtils.unsignedIntByteLength(fieldValue.length);
      size += bytes.length;
      final ByteBuffer buf = ByteBuffer.allocate(size);
      VarintUtils.writeUnsignedInt(fieldValue.length, buf);
      buf.put(bytes);
      return buf.array();
    }
  }
}
