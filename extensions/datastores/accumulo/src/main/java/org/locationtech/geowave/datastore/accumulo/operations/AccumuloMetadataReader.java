/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.accumulo.util.AccumuloUtils;
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
      if ((query.getAuthorizations() != null) && (query.getAuthorizations().length > 0)) {
        operations.ensureAuthorizations(null, query.getAuthorizations());
      }
      final BatchScanner scanner =
          operations.createBatchScanner(
              AbstractGeoWavePersistence.METADATA_TABLE,
              query.getAuthorizations());
      final String columnFamily = metadataType.id();
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
        if (query.isPrefix()) {
          ranges.add(Range.prefix(new Text(query.getPrimaryId())));
        } else {
          ranges.add(Range.exact(new Text(query.getPrimaryId())));
        }
      } else if (query.hasPrimaryIdRanges()) {
        ranges.addAll(
            AccumuloUtils.byteArrayRangesToAccumuloRanges(
                Arrays.asList(query.getPrimaryIdRanges())));
      } else {
        ranges.add(new Range());
      }
      scanner.setRanges(ranges);

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
  }
}
