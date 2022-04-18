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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.WriteResults;
import org.locationtech.geowave.core.store.api.Writer;
import com.google.common.collect.Maps;

public class IndexCompositeWriter<T> implements Writer<T> {
  final Writer<T>[] writers;

  public IndexCompositeWriter(final Writer<T>[] writers) {
    super();
    this.writers = writers;
  }

  @Override
  public void close() {
    for (final Writer<T> indexWriter : writers) {
      indexWriter.close();
    }
  }

  @Override
  public WriteResults write(final T entry) {
    return internalWrite(entry, (w -> w.write(entry)));
  }

  @Override
  public WriteResults write(final T entry, final VisibilityHandler visibilityHandler) {
    return internalWrite(entry, (w -> w.write(entry, visibilityHandler)));
  }

  protected WriteResults internalWrite(
      final T entry,
      final Function<Writer<T>, WriteResults> internalWriter) {
    final Map<String, List<SinglePartitionInsertionIds>> insertionIdsPerIndex = new HashMap<>();
    for (final Writer<T> indexWriter : writers) {
      final WriteResults ids = internalWriter.apply(indexWriter);
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
  public Index[] getIndices() {
    final List<Index> ids = new ArrayList<>();
    for (final Writer<T> indexWriter : writers) {
      ids.addAll(Arrays.asList(indexWriter.getIndices()));
    }
    return ids.toArray(new Index[ids.size()]);
  }

  @Override
  public void flush() {
    for (final Writer<T> indexWriter : writers) {
      indexWriter.flush();
    }
  }
}
