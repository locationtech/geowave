/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.client.Result;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;

public class HBaseRow implements GeoWaveRow {
  private final GeoWaveKey key;
  private final GeoWaveValue[] fieldValues;

  public HBaseRow(final Result result, final int partitionKeyLength) {
    // TODO: GEOWAVE-1018 - can we do something more clever that lazily
    // parses only whats required by the getter (and caches anything else
    // that is parsed)?
    key = new GeoWaveKeyImpl(result.getRow(), partitionKeyLength);

    final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowMapping =
        result.getMap();
    final List<GeoWaveValue> fieldValueList = new ArrayList();

    for (final Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cfEntry : rowMapping.entrySet()) {
      for (final Entry<byte[], NavigableMap<Long, byte[]>> cqEntry : cfEntry.getValue().entrySet()) {
        for (final Entry<Long, byte[]> cqEntryValue : cqEntry.getValue().entrySet()) {
          final byte[] byteValue = cqEntryValue.getValue();
          final byte[] qualifier = cqEntry.getKey();

          fieldValueList.add(new GeoWaveValueImpl(qualifier, null, byteValue));
        }
      }
    }

    fieldValues = new GeoWaveValue[fieldValueList.size()];
    int i = 0;

    for (final GeoWaveValue gwValue : fieldValueList) {
      fieldValues[i++] = gwValue;
    }
  }

  @Override
  public byte[] getDataId() {
    return key.getDataId();
  }

  @Override
  public short getAdapterId() {
    return key.getAdapterId();
  }

  @Override
  public byte[] getSortKey() {
    return key.getSortKey();
  }

  @Override
  public byte[] getPartitionKey() {
    return key.getPartitionKey();
  }

  @Override
  public int getNumberOfDuplicates() {
    return key.getNumberOfDuplicates();
  }

  @Override
  public GeoWaveValue[] getFieldValues() {
    return fieldValues;
  }
}
