/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.metadata.MetadataIterators;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBMetadataTable;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class RocksDBMetadataReader implements MetadataReader {
  private final RocksDBMetadataTable table;
  private final MetadataType metadataType;

  public RocksDBMetadataReader(final RocksDBMetadataTable table, final MetadataType metadataType) {
    this.table = table;
    this.metadataType = metadataType;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(final MetadataQuery query) {
    CloseableIterator<GeoWaveMetadata> originalResults;
    Iterator<GeoWaveMetadata> resultsIt;
    if (query.hasPrimaryId()) {
      originalResults = table.iterator(query.getPrimaryId());
      resultsIt = originalResults;
    } else if (query.hasPrimaryIdRanges()) {
      final List<CloseableIterator<GeoWaveMetadata>> rangeIterators =
          Arrays.stream(query.getPrimaryIdRanges()).map(table::iterator).collect(
              Collectors.toList());
      originalResults =
          new CloseableIteratorWrapper<>(
              (() -> rangeIterators.forEach(CloseableIterator::close)),
              Iterators.concat(rangeIterators.iterator()));
      resultsIt = originalResults;
    } else {
      originalResults = table.iterator();
      resultsIt = originalResults;
    }
    if (query.hasPrimaryId() || query.hasSecondaryId()) {
      resultsIt = Iterators.filter(resultsIt, new Predicate<GeoWaveMetadata>() {

        @Override
        public boolean apply(final GeoWaveMetadata input) {
          if (query.hasPrimaryId()
              && !DataStoreUtils.startsWithIfPrefix(
                  input.getPrimaryId(),
                  query.getPrimaryId(),
                  query.isPrefix())) {
            return false;
          }
          if (query.hasSecondaryId()
              && !Arrays.equals(input.getSecondaryId(), query.getSecondaryId())) {
            return false;
          }
          return true;
        }
      });
    }
    final CloseableIterator<GeoWaveMetadata> retVal =
        new CloseableIteratorWrapper<>(originalResults, resultsIt);
    if (metadataType.isStatValues()) {
      return MetadataIterators.clientVisibilityFilter(retVal, query.getAuthorizations());
    }
    return retVal;
  }
}
