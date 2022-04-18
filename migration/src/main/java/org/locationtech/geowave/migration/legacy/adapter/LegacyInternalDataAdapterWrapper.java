/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration.legacy.adapter;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.migration.legacy.adapter.vector.LegacyFeatureDataAdapter;

public class LegacyInternalDataAdapterWrapper<T> implements InternalDataAdapter<T> {
  private InternalDataAdapter<T> updatedAdapter;
  private DataTypeAdapter<T> adapter;
  private short adapterId;

  public LegacyInternalDataAdapterWrapper() {}

  public LegacyInternalDataAdapterWrapper(final DataTypeAdapter<T> adapter, final short adapterId) {
    this.adapter = adapter;
    this.adapterId = adapterId;
  }

  public InternalDataAdapter<T> getUpdatedAdapter() {
    return updatedAdapter;
  }

  @Override
  public byte[] toBinary() {
    byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
    ByteBuffer buffer = ByteBuffer.allocate(adapterBytes.length + 2);
    buffer.putShort(adapterId);
    buffer.put(adapterBytes);
    return buffer.array();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void fromBinary(final byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    adapterId = buffer.getShort();
    byte[] adapterBytes = new byte[buffer.remaining()];
    buffer.get(adapterBytes);
    adapter = (DataTypeAdapter<T>) PersistenceUtils.fromBinary(adapterBytes);
    VisibilityHandler visibilityHandler = new UnconstrainedVisibilityHandler();
    if (adapter instanceof LegacyFeatureDataAdapter) {
      visibilityHandler = ((LegacyFeatureDataAdapter) adapter).getVisibilityHandler();
      adapter = (DataTypeAdapter<T>) ((LegacyFeatureDataAdapter) adapter).getUpdatedAdapter();
    }
    this.updatedAdapter = adapter.asInternalAdapter(adapterId, visibilityHandler);
  }

  @Override
  public VisibilityHandler getVisibilityHandler() {
    return updatedAdapter.getVisibilityHandler();
  }

  @Override
  public short getAdapterId() {
    return updatedAdapter.getAdapterId();
  }

  @Override
  public String getTypeName() {
    return updatedAdapter.getTypeName();
  }

  @Override
  public byte[] getDataId(T entry) {
    return updatedAdapter.getDataId(entry);
  }

  @Override
  public Object getFieldValue(T entry, String fieldName) {
    return updatedAdapter.getFieldValue(entry, fieldName);
  }

  @Override
  public Class<T> getDataClass() {
    return updatedAdapter.getDataClass();
  }

  @Override
  public RowBuilder<T> newRowBuilder(FieldDescriptor<?>[] outputFieldDescriptors) {
    return updatedAdapter.newRowBuilder(outputFieldDescriptors);
  }

  @Override
  public FieldDescriptor<?>[] getFieldDescriptors() {
    return updatedAdapter.getFieldDescriptors();
  }

  @Override
  public FieldDescriptor<?> getFieldDescriptor(String fieldName) {
    return updatedAdapter.getFieldDescriptor(fieldName);
  }

  @Override
  public DataTypeAdapter<T> getAdapter() {
    return updatedAdapter.getAdapter();
  }

  @Override
  public int getPositionOfOrderedField(CommonIndexModel model, String fieldName) {
    return updatedAdapter.getPositionOfOrderedField(model, fieldName);
  }

  @Override
  public String getFieldNameForPosition(CommonIndexModel model, int position) {
    return updatedAdapter.getFieldNameForPosition(model, position);
  }

  @Override
  public AdapterPersistenceEncoding encode(
      T entry,
      AdapterToIndexMapping indexMapping,
      Index index) {
    return updatedAdapter.encode(entry, indexMapping, index);
  }

  @Override
  public T decode(
      IndexedAdapterPersistenceEncoding data,
      AdapterToIndexMapping indexMapping,
      Index index) {
    return updatedAdapter.decode(data, indexMapping, index);
  }

  @Override
  public boolean isCommonIndexField(AdapterToIndexMapping indexMapping, String fieldName) {
    return updatedAdapter.isCommonIndexField(indexMapping, fieldName);
  }

}
