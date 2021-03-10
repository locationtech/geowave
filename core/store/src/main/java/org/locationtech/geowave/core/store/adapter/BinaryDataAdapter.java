/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.SingleFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public class BinaryDataAdapter implements DataTypeAdapter<Pair<byte[], byte[]>> {
  protected static final String SINGLETON_FIELD_NAME = "FIELD";
  private static final FieldReader<Object> READER = new FieldReader<Object>() {
    @Override
    public Object readField(final byte[] fieldData) {
      return fieldData;
    }
  };
  private static final FieldWriter WRITER = new FieldWriter<Pair<byte[], byte[]>, byte[]>() {
    @Override
    public byte[] writeField(final byte[] fieldValue) {
      return fieldValue;
    }
  };
  private String typeName;

  public BinaryDataAdapter() {
    typeName = null;
  }

  public BinaryDataAdapter(final String typeName) {
    super();
    this.typeName = typeName;
  }

  @Override
  public FieldReader<Object> getReader(final String fieldName) {
    return READER;
  }

  @Override
  public FieldWriter<Pair<byte[], byte[]>, Object> getWriter(final String fieldName) {
    return WRITER;
  }

  @Override
  public Pair<byte[], byte[]> decode(
      final IndexedAdapterPersistenceEncoding data,
      final Index index) {
    return Pair.of(
        data.getDataId(),
        (byte[]) data.getAdapterExtendedData().getValue(SINGLETON_FIELD_NAME));
  }

  @Override
  public AdapterPersistenceEncoding encode(
      final Pair<byte[], byte[]> entry,
      final CommonIndexModel indexModel) {
    return new AdapterPersistenceEncoding(
        getDataId(entry),
        new MultiFieldPersistentDataset<>(),
        new SingleFieldPersistentDataset<>(SINGLETON_FIELD_NAME, entry.getValue()));
  }

  @Override
  public boolean isCommonIndexField(final CommonIndexModel model, final String fieldName) {
    return false;
  }

  @Override
  public int getPositionOfOrderedField(final CommonIndexModel model, final String fieldName) {
    return 0;
  }

  @Override
  public String getFieldNameForPosition(final CommonIndexModel model, final int position) {
    return SINGLETON_FIELD_NAME;
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
  public int getFieldCount() {
    return 1;
  }

  @Override
  public Class<?> getFieldClass(int fieldIndex) {
    return byte[].class;
  }

  @Override
  public String getFieldName(int fieldIndex) {
    return SINGLETON_FIELD_NAME;
  }

  @Override
  public Object getFieldValue(Pair<byte[], byte[]> entry, String fieldName) {
    return entry.getValue();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Class getDataClass() {
    return Pair.class;
  }
}
