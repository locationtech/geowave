/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.util;

import java.util.Iterator;
import java.util.Map;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewritingMergingEntryIterator<T> extends MergingEntryIterator<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RewritingMergingEntryIterator.class);

  private final RowWriter writer;
  private final RowDeleter deleter;

  public RewritingMergingEntryIterator(
      final PersistentAdapterStore adapterStore,
      final Index index,
      final Iterator<GeoWaveRow> scannerIt,
      final Map<Short, RowMergingDataAdapter> mergingAdapters,
      final RowWriter writer,
      final RowDeleter deleter) {
    super(adapterStore, index, scannerIt, null, null, mergingAdapters, null, null);
    this.writer = writer;
    this.deleter = deleter;
  }

  @Override
  protected GeoWaveRow mergeSingleRowValues(
      final GeoWaveRow singleRow,
      final RowTransform rowTransform) {
    if (singleRow.getFieldValues().length < 2) {
      return singleRow;
    }
    deleter.delete(singleRow);
    deleter.flush();
    final GeoWaveRow merged = super.mergeSingleRowValues(singleRow, rowTransform);
    writer.write(merged);
    return merged;
  }
}
