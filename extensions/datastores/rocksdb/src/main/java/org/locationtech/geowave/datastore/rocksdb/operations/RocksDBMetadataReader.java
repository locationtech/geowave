/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.rocksdb.operations;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import java.util.Arrays;
import java.util.Iterator;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.StatisticsRowIterator;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBMetadataTable;

public class RocksDBMetadataReader implements MetadataReader {
  private final RocksDBMetadataTable table;
  private final MetadataType metadataType;

  public RocksDBMetadataReader(final RocksDBMetadataTable table, final MetadataType metadataType) {
    this.table = table;
    this.metadataType = metadataType;
  }

  public CloseableIterator<GeoWaveMetadata> query(
      final MetadataQuery query,
      final boolean mergeStats) {
    CloseableIterator<GeoWaveMetadata> originalResults;
    Iterator<GeoWaveMetadata> resultsIt;
    if (query.hasPrimaryId()) {
      if (query.hasSecondaryId()) {
        originalResults = table.iterator(query.getPrimaryId(), query.getSecondaryId());
        resultsIt = originalResults;
      } else {
        originalResults = table.iterator(query.getPrimaryId());
        resultsIt = originalResults;
      }
    } else {
      originalResults = table.iterator();
      resultsIt = originalResults;
    }
    if (query.hasPrimaryId() || query.hasSecondaryId()) {
      resultsIt = Iterators.filter(resultsIt, new Predicate<GeoWaveMetadata>() {

        @Override
        public boolean apply(final GeoWaveMetadata input) {
          if (query.hasPrimaryId() && !startsWith(input.getPrimaryId(), query.getPrimaryId())) {
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
    final boolean isStats = MetadataType.STATS.equals(metadataType) && mergeStats;
    final CloseableIterator<GeoWaveMetadata> retVal =
        new CloseableIteratorWrapper<>(originalResults, resultsIt);
    return isStats ? new StatisticsRowIterator(retVal, query.getAuthorizations()) : retVal;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(final MetadataQuery query) {
    return query(query, true);
  }

  public static boolean startsWith(final byte[] source, final byte[] match) {

    if (match.length > (source.length)) {
      return false;
    }

    for (int i = 0; i < match.length; i++) {
      if (source[i] != match[i]) {
        return false;
      }
    }
    return true;
  }
}
