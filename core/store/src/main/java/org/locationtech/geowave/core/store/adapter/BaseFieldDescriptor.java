/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import com.beust.jcommander.internal.Sets;

/**
 * Provides a base implementation for adapter field descriptors.
 *
 * @param <T> the adapter field type
 */
public class BaseFieldDescriptor<T> implements FieldDescriptor<T> {
  private Class<T> bindingClass;
  private String fieldName;
  private Set<IndexDimensionHint> indexHints;

  public BaseFieldDescriptor() {}

  public BaseFieldDescriptor(
      final Class<T> bindingClass,
      final String fieldName,
      final Set<IndexDimensionHint> indexHints) {
    this.bindingClass = bindingClass;
    this.fieldName = fieldName;
    this.indexHints = indexHints;
  }

  @Override
  public Class<T> bindingClass() {
    return bindingClass;
  }

  @Override
  public String fieldName() {
    return fieldName;
  }

  @Override
  public Set<IndexDimensionHint> indexHints() {
    return indexHints;
  }

  @Override
  public byte[] toBinary() {
    final byte[] classBytes = StringUtils.stringToBinary(bindingClass.getName());

    final byte[] fieldNameBytes = StringUtils.stringToBinary(fieldName);

    final String[] hintStrings =
        indexHints.stream().map(hint -> hint.getHintString()).toArray(String[]::new);
    final byte[] hintBytes = StringUtils.stringsToBinary(hintStrings);

    final ByteBuffer buffer =
        ByteBuffer.allocate(
            VarintUtils.unsignedShortByteLength((short) classBytes.length)
                + VarintUtils.unsignedShortByteLength((short) fieldNameBytes.length)
                + VarintUtils.unsignedShortByteLength((short) hintBytes.length)
                + classBytes.length
                + fieldNameBytes.length
                + hintBytes.length);
    VarintUtils.writeUnsignedShort((short) classBytes.length, buffer);
    buffer.put(classBytes);
    VarintUtils.writeUnsignedShort((short) fieldNameBytes.length, buffer);
    buffer.put(fieldNameBytes);
    VarintUtils.writeUnsignedShort((short) hintBytes.length, buffer);
    buffer.put(hintBytes);
    return buffer.array();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void fromBinary(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);

    final byte[] classBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(classBytes);
    final String className = StringUtils.stringFromBinary(classBytes);
    try {
      bindingClass = (Class) Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Unable to deserialize class for field descriptor: " + className);
    }

    final byte[] fieldNameBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(fieldNameBytes);
    fieldName = StringUtils.stringFromBinary(fieldNameBytes);

    final byte[] hintBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(hintBytes);
    final String[] hintStrings = StringUtils.stringsFromBinary(hintBytes);
    indexHints = Sets.newHashSet();
    Arrays.stream(hintStrings).forEach(hint -> indexHints.add(new IndexDimensionHint(hint)));
  }
}
