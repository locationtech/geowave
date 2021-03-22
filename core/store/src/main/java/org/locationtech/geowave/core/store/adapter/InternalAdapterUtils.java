/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentValue;

class InternalAdapterUtils {
  protected static <N, I> PersistentValue<?>[] indexValueToNativeValues(
      final IndexFieldMapper<N, I> fieldMapper,
      final DataTypeAdapter<?> adapter,
      final I value) throws IllegalAccessException {
    final List<?> adapterValues = fieldMapper.toAdapter(value);
    final String[] adapterFields = fieldMapper.getAdapterFields();
    if (adapterValues.size() != adapterFields.length) {
      throw new IllegalAccessException(
          String.format(
              "Number of fields (%d) must match number of values (%d)",
              adapterFields.length,
              adapterValues.size()));
    }
    final PersistentValue<?>[] retVal = new PersistentValue[adapterValues.size()];
    for (int i = 0; i < retVal.length; i++) {
      retVal[i] = new PersistentValue<>(adapterFields[i], adapterValues.get(i));
    }
    return retVal;
  }

  @SuppressWarnings("unchecked")
  protected static <T, N, I> Object entryToIndexValue(
      final IndexFieldMapper<N, I> fieldMapper,
      final DataTypeAdapter<T> adapter,
      final T entry) {
    return fieldMapper.toIndex(
        (List<N>) Arrays.stream(fieldMapper.getAdapterFields()).map(
            fieldName -> adapter.getFieldValue(entry, fieldName)).collect(Collectors.toList()));
  }

  @SuppressWarnings("unchecked")
  protected static <T, N, I> Object entryToIndexValue(
      final IndexFieldMapper<N, I> fieldMapper,
      final DataTypeAdapter<T> adapter,
      final PersistentDataset<Object> adapterPersistenceEncoding) {
    return fieldMapper.toIndex(
        (List<N>) Arrays.stream(fieldMapper.getAdapterFields()).map(
            adapterPersistenceEncoding::getValue).collect(Collectors.toList()));
  }
}
