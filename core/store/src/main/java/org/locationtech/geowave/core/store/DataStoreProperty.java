/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

/**
 * A property for storing arbitrary information about a data store. Properties are unique by key,
 * and the value can be any class that is supported by a {@link FieldReader} and {@link FieldWriter}
 * implementation.
 */
public class DataStoreProperty implements Persistable {

  private String key;
  private Object value;

  public DataStoreProperty() {}

  public DataStoreProperty(final String key, final Object value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public byte[] toBinary() {
    final byte[] keyBytes = StringUtils.stringToBinary(key);
    final byte[] classBytes = StringUtils.stringToBinary(value.getClass().getName());
    final byte[] valueBytes;
    if (value instanceof Persistable) {
      valueBytes = PersistenceUtils.toBinary((Persistable) value);
    } else {
      final FieldWriter<Object> writer =
          (FieldWriter<Object>) FieldUtils.getDefaultWriterForClass(value.getClass());
      valueBytes = writer.writeField(value);
    }
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(keyBytes.length)
                + VarintUtils.unsignedIntByteLength(classBytes.length)
                + VarintUtils.unsignedIntByteLength(valueBytes.length)
                + keyBytes.length
                + classBytes.length
                + valueBytes.length);
    VarintUtils.writeUnsignedInt(keyBytes.length, buffer);
    buffer.put(keyBytes);
    VarintUtils.writeUnsignedInt(classBytes.length, buffer);
    buffer.put(classBytes);
    VarintUtils.writeUnsignedInt(valueBytes.length, buffer);
    buffer.put(valueBytes);
    return buffer.array();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte[] keyBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(keyBytes);
    final byte[] classBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(classBytes);
    final byte[] valueBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(valueBytes);
    key = StringUtils.stringFromBinary(keyBytes);
    final String className = StringUtils.stringFromBinary(classBytes);
    try {
      final Class<?> valueClass = Class.forName(className);
      if (Persistable.class.isAssignableFrom(valueClass)) {
        value = PersistenceUtils.fromBinary(valueBytes);
      } else {
        final FieldReader<Object> reader =
            (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(valueClass);
        value = reader.readField(valueBytes);
      }
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Unable to find class for property: " + className);
    }
  }

}
