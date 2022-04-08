/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.Map.Entry;
import java.util.Set;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.index.CommonIndexModel;

/**
 * This is an implements of persistence encoding that also contains all of the extended data values
 * used to form the native type supported by this adapter. It also contains information about the
 * persisted object within a particular index such as the insertion ID in the index and the number
 * of duplicates for this entry in the index, and is used when reading data from the index.
 */
public class IndexedAdapterPersistenceEncoding extends AbstractAdapterPersistenceEncoding {
  public IndexedAdapterPersistenceEncoding(
      final short adapterId,
      final byte[] dataId,
      final byte[] partitionKey,
      final byte[] sortKey,
      final int duplicateCount,
      final PersistentDataset<Object> commonData,
      final PersistentDataset<byte[]> unknownData,
      final PersistentDataset<Object> adapterExtendedData) {
    super(
        adapterId,
        dataId,
        partitionKey,
        sortKey,
        duplicateCount,
        commonData,
        unknownData,
        adapterExtendedData);
  }

  @Override
  public void convertUnknownValues(
      final InternalDataAdapter<?> adapter,
      final CommonIndexModel model) {
    final Set<Entry<String, byte[]>> unknownDataValues = getUnknownData().getValues().entrySet();
    for (final Entry<String, byte[]> v : unknownDataValues) {
      final FieldReader<Object> reader = adapter.getReader(v.getKey());
      final Object value = reader.readField(v.getValue());
      adapterExtendedData.addValue(v.getKey(), value);
    }
  }
}
