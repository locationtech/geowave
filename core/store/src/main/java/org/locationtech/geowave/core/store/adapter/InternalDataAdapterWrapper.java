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
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

public class InternalDataAdapterWrapper<T> implements InternalDataAdapter<T> {
  private DataTypeAdapter<T> adapter;
  private short adapterId;

  public InternalDataAdapterWrapper() {}

  public InternalDataAdapterWrapper(final DataTypeAdapter<T> adapter, final short adapterId) {
    this.adapter = adapter;
    this.adapterId = adapterId;
  }

  @Override
  public FieldWriter<T, Object> getWriter(final String fieldName) {
    return adapter.getWriter(fieldName);
  }

  @Override
  public short getAdapterId() {
    return adapterId;
  }

  @Override
  public byte[] toBinary() {
    byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    ByteBuffer buffer = ByteBuffer.allocate(adapterBytes.length + 2);
    buffer.putShort(adapterId);
    buffer.put(adapterBytes);
    return buffer.array();
  }

  @Override
  public FieldReader<Object> getReader(final String fieldName) {
    return adapter.getReader(fieldName);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void fromBinary(final byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    adapterId = buffer.getShort();
    byte[] adapterBytes = new byte[buffer.remaining()];
    buffer.get(adapterBytes);
    adapter = (DataTypeAdapter<T>) PersistenceUtils.fromBinary(adapterBytes);
  }

  @Override
  public String getTypeName() {
    return adapter.getTypeName();
  }

  @Override
  public byte[] getDataId(final T entry) {
    return adapter.getDataId(entry);
  }

  @Override
  public T decode(final IndexedAdapterPersistenceEncoding data, final Index index) {
    return adapter.decode(data, index);
  }

  @Override
  public AdapterPersistenceEncoding encode(final T entry, final CommonIndexModel indexModel) {
    final AdapterPersistenceEncoding retVal = adapter.encode(entry, indexModel);
    retVal.setInternalAdapterId(adapterId);
    return retVal;
  }

  @Override
  public boolean isCommonIndexField(final CommonIndexModel indexModel, final String fieldName) {
    return adapter.isCommonIndexField(indexModel, fieldName);
  }

  @Override
  public int getPositionOfOrderedField(final CommonIndexModel model, final String fieldName) {
    return adapter.getPositionOfOrderedField(model, fieldName);
  }

  @Override
  public String getFieldNameForPosition(final CommonIndexModel model, final int position) {
    return adapter.getFieldNameForPosition(model, position);
  }

  @Override
  public DataTypeAdapter<T> getAdapter() {
    return adapter;
  }

  @Override
  public int getFieldCount() {
    return adapter.getFieldCount();
  }

  @Override
  public Class<?> getFieldClass(int fieldIndex) {
    return adapter.getFieldClass(fieldIndex);
  }

  @Override
  public String getFieldName(int fieldIndex) {
    return adapter.getFieldName(fieldIndex);
  }

  @Override
  public Object getFieldValue(T entry, String fieldName) {
    return adapter.getFieldValue(entry, fieldName);
  }

  @Override
  public Class<T> getDataClass() {
    return adapter.getDataClass();
  }
}
