/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.RowBuilder;

public class BinaryDataAdapter implements DataTypeAdapter<Pair<byte[], byte[]>> {
  protected static final String SINGLETON_FIELD_NAME = "FIELD";
  protected static final FieldDescriptor<byte[]> SINGLETON_FIELD_DESCRIPTOR =
      new FieldDescriptorBuilder<>(byte[].class).fieldName(SINGLETON_FIELD_NAME).build();
  protected static final FieldDescriptor<?>[] SINGLETON_FIELD_DESCRIPTOR_ARRAY =
      new FieldDescriptor[] {SINGLETON_FIELD_DESCRIPTOR};
  private String typeName;

  public BinaryDataAdapter() {
    typeName = null;
  }

  public BinaryDataAdapter(final String typeName) {
    super();
    this.typeName = typeName;
  }

  @Override
  public byte[] toBinary() {
    return StringUtils.stringToBinary(typeName);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    typeName = StringUtils.stringFromBinary(bytes);
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public byte[] getDataId(final Pair<byte[], byte[]> entry) {
    return entry.getKey();
  }

  @Override
  public Object getFieldValue(final Pair<byte[], byte[]> entry, final String fieldName) {
    return entry.getValue();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Class getDataClass() {
    return Pair.class;
  }

  @Override
  public RowBuilder<Pair<byte[], byte[]>> newRowBuilder(
      final FieldDescriptor<?>[] outputFieldDescriptors) {
    return new BinaryDataRowBuilder();
  }

  @Override
  public FieldDescriptor<?>[] getFieldDescriptors() {
    return SINGLETON_FIELD_DESCRIPTOR_ARRAY;
  }

  @Override
  public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
    if (SINGLETON_FIELD_NAME.equals(fieldName)) {
      return SINGLETON_FIELD_DESCRIPTOR;
    }
    return null;
  }

  protected static class BinaryDataRowBuilder implements RowBuilder<Pair<byte[], byte[]>> {
    protected byte[] fieldValue;

    @Override
    public void setField(final String fieldName, final Object fieldValue) {
      if (SINGLETON_FIELD_NAME.equals(fieldName)
          && ((fieldValue == null) || (fieldValue instanceof byte[]))) {
        this.fieldValue = (byte[]) fieldValue;
      }
    }

    @Override
    public void setFields(final Map<String, Object> values) {
      if (values.containsKey(SINGLETON_FIELD_NAME)) {
        final Object obj = values.get(SINGLETON_FIELD_NAME);
        setField(SINGLETON_FIELD_NAME, obj);
      }
    }

    @Override
    public Pair<byte[], byte[]> buildRow(final byte[] dataId) {
      return Pair.of(dataId, fieldValue);
    }

  }
}
