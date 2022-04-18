/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
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

class InternalAdapterUtils {
  @SuppressWarnings("unchecked")
  protected static <T, N, I> Object entryToIndexValue(
      final IndexFieldMapper<N, I> fieldMapper,
      final DataTypeAdapter<T> adapter,
      final T entry) {
    List<N> fieldValues =
        (List<N>) Arrays.stream(fieldMapper.getAdapterFields()).map(
            fieldName -> adapter.getFieldValue(entry, fieldName)).collect(Collectors.toList());
    if (fieldValues.contains(null)) {
      return null;
    }
    return fieldMapper.toIndex(fieldValues);
  }

  @SuppressWarnings("unchecked")
  protected static <T, N, I> Object entryToIndexValue(
      final IndexFieldMapper<N, I> fieldMapper,
      final DataTypeAdapter<T> adapter,
      final PersistentDataset<Object> adapterPersistenceEncoding) {
    final List<N> fieldValues =
        (List<N>) Arrays.stream(fieldMapper.getAdapterFields()).map(
            adapterPersistenceEncoding::getValue).collect(Collectors.toList());
    if (fieldValues.contains(null)) {
      return null;
    }
    return fieldMapper.toIndex(fieldValues);
  }
}
