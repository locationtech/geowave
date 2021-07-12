/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.util.GenericTypeResolver;
import com.google.common.collect.Maps;

/**
 * Wraps a {@link BasicDataTypeAdapter}, making it compatible with the full {@link DataTypeAdapter}
 * interface.
 *
 * @param <T> the entry data type
 */
public abstract class BasicDataTypeAdapter<T> implements DataTypeAdapter<T> {

  private String typeName = null;
  private FieldDescriptor<?>[] fieldDescriptors = null;
  private String dataIDField = null;
  private Map<String, Integer> fieldDescriptorIndices = Maps.newHashMap();

  public BasicDataTypeAdapter() {}

  public BasicDataTypeAdapter(
      final String typeName,
      final FieldDescriptor<?>[] fieldDescriptors,
      final String dataIDField) {
    this.typeName = typeName;
    if (fieldDescriptors == null || fieldDescriptors.length == 0) {
      throw new IllegalArgumentException("There must be at least one field in a data type.");
    }
    this.fieldDescriptors = fieldDescriptors;
    populateFieldDescriptorIndices();
    if (!fieldDescriptorIndices.containsKey(dataIDField)) {
      throw new IllegalArgumentException("Unable to find data ID field in field descriptor list.");
    }
    this.dataIDField = dataIDField;
  }

  private void populateFieldDescriptorIndices() {
    for (int i = 0; i < fieldDescriptors.length; i++) {
      fieldDescriptorIndices.put(fieldDescriptors[i].fieldName(), i);
    }
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public byte[] getDataId(T entry) {
    return StringUtils.stringToBinary(getFieldValue(entry, dataIDField).toString());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Class<T> getDataClass() {
    return (Class) GenericTypeResolver.resolveTypeArgument(
        this.getClass(),
        BasicDataTypeAdapter.class);
  }

  @Override
  public RowBuilder<T> newRowBuilder(FieldDescriptor<?>[] outputFieldDescriptors) {
    return new RowBuilder<T>() {

      private Object[] values = new Object[outputFieldDescriptors.length];

      @Override
      public void setField(String fieldName, Object fieldValue) {
        values[fieldDescriptorIndices.get(fieldName)] = fieldValue;
      }

      @Override
      public void setFields(Map<String, Object> valueMap) {
        valueMap.entrySet().forEach(
            entry -> values[fieldDescriptorIndices.get(entry.getKey())] = entry.getValue());
      }

      @Override
      public T buildRow(byte[] dataId) {
        T obj = buildObject(values);
        Arrays.fill(values, null);
        return obj;
      }

    };
  }

  public abstract T buildObject(final Object[] fieldValues);

  @Override
  public FieldDescriptor<?>[] getFieldDescriptors() {
    return fieldDescriptors;
  }

  @Override
  public FieldDescriptor<?> getFieldDescriptor(String fieldName) {
    final Integer index = fieldDescriptorIndices.get(fieldName);
    if (index == null) {
      return null;
    }
    return fieldDescriptors[index];
  }

  @Override
  public byte[] toBinary() {
    final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
    final byte[] fieldDescriptorBytes = PersistenceUtils.toBinary(fieldDescriptors);
    final byte[] dataIDFieldBytes = StringUtils.stringToBinary(dataIDField);
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(typeNameBytes.length)
                + VarintUtils.unsignedIntByteLength(fieldDescriptorBytes.length)
                + VarintUtils.unsignedIntByteLength(dataIDFieldBytes.length)
                + typeNameBytes.length
                + fieldDescriptorBytes.length
                + dataIDFieldBytes.length);
    VarintUtils.writeUnsignedInt(typeNameBytes.length, buffer);
    buffer.put(typeNameBytes);
    VarintUtils.writeUnsignedInt(fieldDescriptorBytes.length, buffer);
    buffer.put(fieldDescriptorBytes);
    VarintUtils.writeUnsignedInt(dataIDFieldBytes.length, buffer);
    buffer.put(dataIDFieldBytes);
    return buffer.array();
  }

  @Override
  public void fromBinary(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte[] typeNameBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(typeNameBytes);
    this.typeName = StringUtils.stringFromBinary(typeNameBytes);
    final byte[] fieldDescriptorBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(fieldDescriptorBytes);
    final List<Persistable> fieldDescriptorList =
        PersistenceUtils.fromBinaryAsList(fieldDescriptorBytes);
    this.fieldDescriptors =
        fieldDescriptorList.toArray(new FieldDescriptor<?>[fieldDescriptorList.size()]);
    final byte[] dataIDFieldBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(dataIDFieldBytes);
    this.dataIDField = StringUtils.stringFromBinary(dataIDFieldBytes);
    populateFieldDescriptorIndices();
  }

}
