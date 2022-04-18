/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter.expression;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

/**
 * An expression representing a raw value, not derived from an adapter entry.
 *
 * @param <V> the class that the expression evaluates to
 */
public abstract class Literal<V> implements Expression<V> {

  protected V literal;

  public Literal() {}

  public Literal(final V literal) {
    this.literal = literal;
  }

  public V getValue() {
    return literal;
  }

  @Override
  public void addReferencedFields(final Set<String> fields) {}

  @Override
  public boolean isLiteral() {
    return true;
  }

  @Override
  public V evaluateValue(final Map<String, Object> fieldValues) {
    return literal;
  }

  @Override
  public <T> V evaluateValue(final DataTypeAdapter<T> adapter, final T entry) {
    return literal;
  }

  @Override
  public String toString() {
    return literal == null ? "null" : literal.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public byte[] toBinary() {
    if (literal == null) {
      return new byte[] {(byte) 0};
    }
    final byte[] classBytes = StringUtils.stringToBinary(literal.getClass().getName());
    final FieldWriter<Object> writer =
        (FieldWriter<Object>) FieldUtils.getDefaultWriterForClass(literal.getClass());
    final byte[] valueBytes = writer.writeField(literal);
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            1
                + VarintUtils.unsignedIntByteLength(classBytes.length)
                + VarintUtils.unsignedIntByteLength(valueBytes.length)
                + classBytes.length
                + valueBytes.length);
    buffer.put((byte) 1);
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
    final byte nullByte = buffer.get();
    if (nullByte == 0) {
      literal = null;
      return;
    }
    final byte[] classBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(classBytes);
    final byte[] valueBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(valueBytes);
    final String className = StringUtils.stringFromBinary(classBytes);
    try {
      final Class<?> valueClass = Class.forName(className);
      final FieldReader<Object> reader =
          (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(valueClass);
      literal = (V) reader.readField(valueBytes);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Unable to find class for literal: " + className);
    }
  }

}
