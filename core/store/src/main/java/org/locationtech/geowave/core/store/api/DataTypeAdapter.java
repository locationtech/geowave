/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.util.Map;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterImpl;
import org.locationtech.geowave.core.store.data.DataReader;
import org.locationtech.geowave.core.store.data.DataWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import com.beust.jcommander.internal.Maps;

/**
 * This interface should be implemented by any custom data type that must be stored in the GeoWave
 * index. It enables storing and retrieving the data, as well as translating the data into values
 * and queries that can be used to index. Additionally, each entry is responsible for providing
 * visibility if applicable.
 *
 * @param <T> The type of entries that this adapter works on.
 */
public interface DataTypeAdapter<T> extends DataReader<Object>, DataWriter<Object>, Persistable {
  /**
   * Return the data adapter's type name. This also must be unique within a datastore.
   *
   * @return the type name which serves as a unique identifier for this adapter
   */
  String getTypeName();

  /**
   * Get a data ID for the entry. This should uniquely identify the entry in the data set.
   *
   * @param entry the entry
   * @return the data ID
   */
  byte[] getDataId(T entry);

  default InternalDataAdapter<T> asInternalAdapter(final short internalAdapterId) {
    return new InternalDataAdapterImpl<>(this, internalAdapterId);
  }

  default InternalDataAdapter<T> asInternalAdapter(
      final short internalAdapterId,
      final VisibilityHandler visibilityHandler) {
    return new InternalDataAdapterImpl<>(this, internalAdapterId, visibilityHandler);
  }

  @SuppressWarnings("unchecked")
  @Override
  default FieldWriter<Object> getWriter(final String fieldName) {
    final FieldDescriptor<?> descriptor = getFieldDescriptor(fieldName);
    if (descriptor == null) {
      throw new IllegalArgumentException("'" + fieldName + "' does not exist for field writer");
    }
    return (FieldWriter<Object>) FieldUtils.getDefaultWriterForClass(descriptor.bindingClass());
  }

  @SuppressWarnings("unchecked")
  @Override
  default FieldReader<Object> getReader(final String fieldName) {
    final FieldDescriptor<?> descriptor = getFieldDescriptor(fieldName);
    if (descriptor == null) {
      throw new IllegalArgumentException("'" + fieldName + "' does not exist for field reader");
    }

    return (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(descriptor.bindingClass());
  }

  /**
   * Returns the value of the field with the given name from the entry.
   *
   * @param entry the entry
   * @param fieldName the field name
   * @return the value of the field on the entry
   */
  Object getFieldValue(T entry, String fieldName);

  /**
   * Return the class that represents the data stored by this adapter.
   *
   * @return the class of the data
   */
  Class<T> getDataClass();

  RowBuilder<T> newRowBuilder(FieldDescriptor<?>[] outputFieldDescriptors);

  FieldDescriptor<?>[] getFieldDescriptors();

  FieldDescriptor<?> getFieldDescriptor(String fieldName);

  default Map<String, String> describe() {
    return Maps.newHashMap();
  }

}
