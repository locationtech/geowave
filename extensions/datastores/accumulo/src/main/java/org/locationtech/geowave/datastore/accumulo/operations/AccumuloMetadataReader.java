/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.operations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.accumulo.util.ScannerClosableWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;

public class AccumuloMetadataReader implements MetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloMetadataReader.class);
  private final AccumuloOperations operations;
  private final DataStoreOptions options;
  private final MetadataType metadataType;

  public AccumuloMetadataReader(
      final AccumuloOperations operations,
      final DataStoreOptions options,
      final MetadataType metadataType) {
    this.operations = operations;
    this.options = options;
    this.metadataType = metadataType;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(final MetadataQuery query) {
    try {
      final BatchScanner scanner =
          operations.createBatchScanner(
              AbstractGeoWavePersistence.METADATA_TABLE,
              query.getAuthorizations());
      final String columnFamily = metadataType.name();
      final byte[] columnQualifier = query.getSecondaryId();
      if (columnFamily != null) {
        if (columnQualifier != null) {
          scanner.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
        } else {
          scanner.fetchColumnFamily(new Text(columnFamily));
        }
      }
      final Collection<Range> ranges = new ArrayList<>();
      if (query.hasPrimaryId()) {
        ranges.add(Range.prefix(new Text(query.getPrimaryId())));
      } else {
        ranges.add(new Range());
      }
      scanner.setRanges(ranges);

      // For stats w/ no server-side support, need to merge here
      if ((metadataType == MetadataType.STATS) && !options.isServerSideLibraryEnabled()) {
        try {
          // final HashMap<Text, Key> keyMap = new HashMap<>();
          final HashMap<Pair<Text, Text>, InternalDataStatistics<?, ?, ?>> mergedDataMap =
              new HashMap<>();
          final Iterator<Entry<Key, Value>> it = scanner.iterator();

          while (it.hasNext()) {
            final Entry<Key, Value> row = it.next();

            final InternalDataStatistics<?, ?, ?> stats =
                (InternalDataStatistics<?, ?, ?>) PersistenceUtils.fromBinary(row.getValue().get());
            final Pair<Text, Text> rowCqPair =
                ImmutablePair.of(row.getKey().getRow(), row.getKey().getColumnQualifier());
            final InternalDataStatistics<?, ?, ?> mergedStats = mergedDataMap.get(rowCqPair);
            stats.setVisibility(row.getKey().getColumnVisibility().getBytes());
            if (mergedStats != null) {
              mergedStats.merge(stats);
              mergedStats.setVisibility(
                  DataStoreUtils.mergeVisibilities(
                      mergedStats.getVisibility(),
                      stats.getVisibility()));
            } else {
              mergedDataMap.put(rowCqPair, stats);
            }
          }

          final List<GeoWaveMetadata> metadataList = new ArrayList<>();
          for (final Entry<Pair<Text, Text>, InternalDataStatistics<?, ?, ?>> entry : mergedDataMap.entrySet()) {
            final Pair<Text, Text> key = entry.getKey();
            final InternalDataStatistics<?, ?, ?> mergedStats = entry.getValue();

            metadataList.add(
                new GeoWaveMetadata(
                    key.getLeft().getBytes(),
                    key.getRight().getBytes(),
                    mergedStats.getVisibility(),
                    PersistenceUtils.toBinary(mergedStats)));
          }

          return new CloseableIterator.Wrapper<>(metadataList.iterator());
        } finally {
          scanner.close();
        }
      }

      return new CloseableIteratorWrapper<>(
          new ScannerClosableWrapper(scanner),
          Iterators.transform(
              scanner.iterator(),
              row -> new GeoWaveMetadata(
                  row.getKey().getRow().getBytes(),
                  row.getKey().getColumnQualifier().getBytes(),
                  row.getKey().getColumnVisibility().getBytes(),
                  row.getValue().get())));
    } catch (final TableNotFoundException e) {
      LOGGER.warn("GeoWave metadata table not found", e);
    }
    return new CloseableIterator.Wrapper<>(Collections.emptyIterator());
  }

  private static class PartialKeyWrapper extends Key {
    public PartialKeyWrapper(final Key other) {
      super(other);
    }

    @Override
    public boolean equals(final Object o) {
      if (o instanceof Key) {
        return super.equals((Key) o, PartialKey.ROW_COLFAM_COLQUAL);
      }
      return false;
    }

    @Override
    public int compareTo(final Key other) {
      return super.compareTo(other, PartialKey.ROW_COLFAM_COLQUAL);
    }

    @Override
    public int hashCode() {
      return WritableComparator.hashBytes(row, row.length)
          + WritableComparator.hashBytes(colFamily, colFamily.length)
          + WritableComparator.hashBytes(colQualifier, colQualifier.length);
    }

    protected void mergeVisibilities(final byte[] visibility) {
      colVisibility = DataStoreUtils.mergeVisibilities(colVisibility, visibility);
    }

  }
}
