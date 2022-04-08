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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.util.GenericTypeResolver;

/**
 * Provides an abstract implementation of the {@link DataTypeAdapter} interface that handles field
 * descriptors, data ID, and type name.
 *
 * @param <T> the entry data type
 */
public abstract class AbstractDataTypeAdapter<T> implements DataTypeAdapter<T> {

  private String typeName = null;
  private FieldDescriptor<?>[] fieldDescriptors = null;
  private FieldDescriptor<?> dataIDFieldDescriptor = null;
  private Map<String, Integer> fieldDescriptorIndices = new HashMap<>();
  private FieldWriter<Object> dataIDWriter = null;
  private FieldReader<Object> dataIDReader = null;

  // Maintain backwards compatibility with 2.0.x
  protected boolean serializeDataIDAsString = false;

  public AbstractDataTypeAdapter() {}

  public AbstractDataTypeAdapter(
      final String typeName,
      final FieldDescriptor<?>[] fieldDescriptors,
      final FieldDescriptor<?> dataIDFieldDescriptor) {
    this.typeName = typeName;
    if (fieldDescriptors == null) {
      throw new IllegalArgumentException("An array of field descriptors must be provided.");
    }
    if (dataIDFieldDescriptor == null) {
      throw new IllegalArgumentException("A data ID field descriptor must be provided.");
    }
    this.fieldDescriptors = fieldDescriptors;
    this.dataIDFieldDescriptor = dataIDFieldDescriptor;
    populateFieldDescriptorIndices();
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

  /**
   * Returns the value of the field with the given name from the entry. If the data ID field name is
   * passed, it is expected that this method will return the value of that field even if the data ID
   * field is not included in the set of field descriptors.
   *
   * @param entry the entry
   * @param fieldName the field name or data ID field name
   * @return the value of the field on the entry
   */
  @Override
  public abstract Object getFieldValue(T entry, String fieldName);


  @SuppressWarnings("unchecked")
  @Override
  public byte[] getDataId(T entry) {
    if (serializeDataIDAsString) {
      return StringUtils.stringToBinary(
          getFieldValue(entry, dataIDFieldDescriptor.fieldName()).toString());
    }
    if (dataIDWriter == null) {
      dataIDWriter =
          (FieldWriter<Object>) FieldUtils.getDefaultWriterForClass(
              dataIDFieldDescriptor.bindingClass());
    }
    return dataIDWriter.writeField(getFieldValue(entry, dataIDFieldDescriptor.fieldName()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Class<T> getDataClass() {
    return (Class) GenericTypeResolver.resolveTypeArgument(
        this.getClass(),
        AbstractDataTypeAdapter.class);
  }

  @SuppressWarnings("unchecked")
  @Override
  public RowBuilder<T> newRowBuilder(FieldDescriptor<?>[] outputFieldDescriptors) {
    if (!serializeDataIDAsString && dataIDReader == null) {
      dataIDReader =
          (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(
              dataIDFieldDescriptor.bindingClass());
    }
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
        final Object dataIDObject =
            serializeDataIDAsString ? dataId : dataIDReader.readField(dataId);
        T obj = buildObject(dataIDObject, values);
        Arrays.fill(values, null);
        return obj;
      }

    };
  }

  public abstract T buildObject(final Object dataId, final Object[] fieldValues);

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

  protected FieldDescriptor<?> getDataIDFieldDescriptor() {
    return dataIDFieldDescriptor;
  }

  @Override
  public byte[] toBinary() {
    final byte[] typeNameBytes = StringUtils.stringToBinary(typeName);
    final byte[] fieldDescriptorBytes = PersistenceUtils.toBinary(fieldDescriptors);
    // Maintain backwards compatibility for 2.0.x
    final byte[] dataIDFieldBytes =
        serializeDataIDAsString ? StringUtils.stringToBinary(dataIDFieldDescriptor.fieldName())
            : new byte[0];
    final byte[] dataIDFieldDescriptorBytes = PersistenceUtils.toBinary(dataIDFieldDescriptor);
    int bufferSize =
        VarintUtils.unsignedIntByteLength(typeNameBytes.length)
            + VarintUtils.unsignedIntByteLength(fieldDescriptorBytes.length)
            + VarintUtils.unsignedIntByteLength(dataIDFieldBytes.length)
            + typeNameBytes.length
            + fieldDescriptorBytes.length
            + dataIDFieldBytes.length;
    if (!serializeDataIDAsString) {
      bufferSize +=
          VarintUtils.unsignedIntByteLength(dataIDFieldDescriptorBytes.length)
              + dataIDFieldDescriptorBytes.length;
    }
    final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    VarintUtils.writeUnsignedInt(typeNameBytes.length, buffer);
    buffer.put(typeNameBytes);
    VarintUtils.writeUnsignedInt(fieldDescriptorBytes.length, buffer);
    buffer.put(fieldDescriptorBytes);
    VarintUtils.writeUnsignedInt(dataIDFieldBytes.length, buffer);
    buffer.put(dataIDFieldBytes);
    if (!serializeDataIDAsString) {
      VarintUtils.writeUnsignedInt(dataIDFieldDescriptorBytes.length, buffer);
      buffer.put(dataIDFieldDescriptorBytes);
    }
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
    final String dataIDField = StringUtils.stringFromBinary(dataIDFieldBytes);
    if (buffer.hasRemaining()) {
      final byte[] dataIDFieldDescriptorBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
      buffer.get(dataIDFieldDescriptorBytes);
      this.dataIDFieldDescriptor =
          (FieldDescriptor<?>) PersistenceUtils.fromBinary(dataIDFieldDescriptorBytes);
    } else {
      for (int i = 0; i < fieldDescriptors.length; i++) {
        if (fieldDescriptors[i].fieldName().equals(dataIDField)) {
          this.dataIDFieldDescriptor = fieldDescriptors[i];
        }
      }
      this.serializeDataIDAsString = true;
    }
    populateFieldDescriptorIndices();
  }

}
