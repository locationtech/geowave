/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index.writer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.store.adapter.IndexDependentDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.WriteResults;
import org.locationtech.geowave.core.store.api.Writer;
import com.google.common.collect.Maps;

public class IndependentAdapterIndexWriter<T> implements Writer<T> {

  final IndexDependentDataAdapter<T> adapter;
  final Index index;
  final VisibilityHandler visibilityHandler;
  final Writer<T> writer;

  public IndependentAdapterIndexWriter(
      final IndexDependentDataAdapter<T> adapter,
      final Index index,
      final VisibilityHandler visibilityHandler,
      final Writer<T> writer) {
    super();
    this.writer = writer;
    this.index = index;
    this.visibilityHandler = visibilityHandler;
    this.adapter = adapter;
  }

  @Override
  public WriteResults write(final T entry, final VisibilityHandler visibilityHandler) {
    return internalWrite(entry, (e -> writer.write(e, visibilityHandler)));
  }

  private WriteResults internalWrite(
      final T entry,
      final Function<T, WriteResults> internalWriter) {
    final Iterator<T> indexedEntries = adapter.convertToIndex(index, entry);
    final Map<String, List<SinglePartitionInsertionIds>> insertionIdsPerIndex = new HashMap<>();
    while (indexedEntries.hasNext()) {
      final WriteResults ids = internalWriter.apply(indexedEntries.next());
      for (final String indexName : ids.getWrittenIndexNames()) {
        List<SinglePartitionInsertionIds> partitionInsertionIds =
            insertionIdsPerIndex.get(indexName);
        if (partitionInsertionIds == null) {
          partitionInsertionIds = new ArrayList<>();
          insertionIdsPerIndex.put(indexName, partitionInsertionIds);
        }
        partitionInsertionIds.addAll(ids.getInsertionIdsWritten(indexName).getPartitionKeys());
      }
    }
    return new WriteResults(Maps.transformValues(insertionIdsPerIndex, v -> new InsertionIds(v)));
  }

  @Override
  public void close() {
    writer.close();
  }

  @Override
  public WriteResults write(final T entry) {
    return internalWrite(entry, (e -> writer.write(e, visibilityHandler)));
  }

  @Override
  public Index[] getIndices() {
    return writer.getIndices();
  }

  @Override
  public void flush() {
    writer.flush();
  }
}
