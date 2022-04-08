/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Determines the visibility of a field by looking up the field in a visibility map.
 */
public class FieldMappedVisibilityHandler implements VisibilityHandler {
  private Map<String, String> fieldVisibilities;

  public FieldMappedVisibilityHandler() {}

  public FieldMappedVisibilityHandler(final Map<String, String> fieldVisibilities) {
    this.fieldVisibilities = fieldVisibilities;
  }

  @Override
  public <T> String getVisibility(
      final DataTypeAdapter<T> adapter,
      final T rowValue,
      final String fieldName) {
    if (fieldVisibilities.containsKey(fieldName)) {
      return fieldVisibilities.get(fieldName);
    }
    return null;
  }

  @Override
  public byte[] toBinary() {
    int byteLength = VarintUtils.unsignedIntByteLength(fieldVisibilities.size());
    final List<byte[]> byteList = Lists.newArrayListWithCapacity(fieldVisibilities.size() * 2);
    for (Entry<String, String> entry : fieldVisibilities.entrySet()) {
      final byte[] keyBytes = StringUtils.stringToBinary(entry.getKey());
      byteList.add(keyBytes);
      byteLength += VarintUtils.unsignedIntByteLength(keyBytes.length);
      byteLength += keyBytes.length;
      final byte[] valueBytes = StringUtils.stringToBinary(entry.getValue());
      byteList.add(valueBytes);
      byteLength += VarintUtils.unsignedIntByteLength(valueBytes.length);
      byteLength += valueBytes.length;
    }
    final ByteBuffer buffer = ByteBuffer.allocate(byteLength);
    VarintUtils.writeUnsignedInt(fieldVisibilities.size(), buffer);
    for (final byte[] bytes : byteList) {
      VarintUtils.writeUnsignedInt(bytes.length, buffer);
      buffer.put(bytes);
    }
    return buffer.array();
  }

  @Override
  public void fromBinary(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final int size = VarintUtils.readUnsignedInt(buffer);
    fieldVisibilities = Maps.newHashMapWithExpectedSize(size);
    for (int i = 0; i < size; i++) {
      final byte[] keyBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
      buffer.get(keyBytes);
      final String key = StringUtils.stringFromBinary(keyBytes);
      final byte[] valueBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
      buffer.get(valueBytes);
      final String value = StringUtils.stringFromBinary(valueBytes);
      fieldVisibilities.put(key, value);
    }
  }
}
