/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.operations;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.datastore.hbase.util.HBaseUtils;
import org.locationtech.geowave.datastore.hbase.util.HBaseUtils.ScannerClosableWrapper;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseMetadataReader implements MetadataReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(HBaseMetadataReader.class);
  private final HBaseOperations operations;
  private final DataStoreOptions options;
  private final MetadataType metadataType;

  public HBaseMetadataReader(
      final HBaseOperations operations,
      final DataStoreOptions options,
      final MetadataType metadataType) {
    this.operations = operations;
    this.options = options;
    this.metadataType = metadataType;
  }

  @Override
  public CloseableIterator<GeoWaveMetadata> query(final MetadataQuery query) {
    final Scan scanner = new Scan();

    try {
      final byte[] columnFamily = StringUtils.stringToBinary(metadataType.id());
      final byte[] columnQualifier = query.getSecondaryId();

      if (columnQualifier != null) {
        scanner.addColumn(columnFamily, columnQualifier);
      } else {
        scanner.addFamily(columnFamily);
      }
      if (query.hasPrimaryIdRanges()) {
        final MultiRowRangeFilter filter =
            operations.getMultiRowRangeFilter(Arrays.asList(query.getPrimaryIdRanges()));
        // TODO performance could be perhaps improved using parallel scanning logic but for now keep
        // it simple
        if (filter.getRowRanges().size() == 1) {
          scanner.withStartRow(filter.getRowRanges().get(0).getStartRow()).withStopRow(
              filter.getRowRanges().get(0).getStopRow());
        } else if (filter.getRowRanges().size() > 1) {
          scanner.setFilter(filter);
          scanner.withStartRow(filter.getRowRanges().get(0).getStartRow()).withStopRow(
              filter.getRowRanges().get(filter.getRowRanges().size() - 1).getStopRow());
        } else {
          return new CloseableIterator.Empty<>();
        }
      } else {
        if (query.hasPrimaryId()) {
          if (query.isPrefix()) {
            scanner.withStartRow(query.getPrimaryId()).withStopRow(
                ByteArrayUtils.getNextPrefix(query.getPrimaryId()));
          } else {
            scanner.withStartRow(query.getPrimaryId()).withStopRow(query.getPrimaryId(), true);
          }
        }
      }
      final boolean clientsideStatsMerge =
          (metadataType.isStatValues()) && !options.isServerSideLibraryEnabled();
      if (clientsideStatsMerge) {
        scanner.readAllVersions(); // Get all versions
      }

      final String[] additionalAuthorizations = query.getAuthorizations();
      if ((additionalAuthorizations != null) && (additionalAuthorizations.length > 0)) {
        scanner.setAuthorizations(new Authorizations(additionalAuthorizations));
      }
      final Iterable<Result> rS =
          operations.getScannedResults(scanner, operations.getMetadataTableName(metadataType));
      final Iterator<GeoWaveMetadata> transformedIt =
          StreamSupport.stream(rS.spliterator(), false).flatMap(result -> {
            byte[] resultantCQ;
            if (columnQualifier == null) {
              final NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(columnFamily);
              if ((familyMap != null) && !familyMap.isEmpty()) {
                if (familyMap.size() > 1) {
                  return familyMap.keySet().stream().map(
                      key -> new GeoWaveMetadata(
                          result.getRow(),
                          key,
                          null,
                          getMergedStats(result, clientsideStatsMerge, columnFamily, key)));
                }
                resultantCQ = familyMap.firstKey();
              } else {
                resultantCQ = new byte[0];
              }
            } else {
              resultantCQ = columnQualifier;
            }
            return Stream.of(
                new GeoWaveMetadata(
                    result.getRow(),
                    resultantCQ,
                    null,
                    getMergedStats(result, clientsideStatsMerge)));
          }).iterator();
      if (rS instanceof ResultScanner) {
        return new CloseableIteratorWrapper<>(
            new ScannerClosableWrapper((ResultScanner) rS),
            transformedIt);
      } else {
        return new CloseableIterator.Wrapper<>(transformedIt);
      }

    } catch (final Exception e) {
      LOGGER.warn("GeoWave metadata table not found", e);
    }
    return new CloseableIterator.Wrapper<>(Collections.emptyIterator());
  }

  private byte[] getMergedStats(
      final Result result,
      final boolean clientsideStatsMerge,
      final byte[] columnFamily,
      final byte[] columnQualifier) {
    final List<Cell> columnCells = result.getColumnCells(columnFamily, columnQualifier);
    if ((columnCells.size() == 1)) {
      return CellUtil.cloneValue(columnCells.get(0));
    }
    return URLClassloaderUtils.toBinary(HBaseUtils.getMergedStats(columnCells));
  }

  private byte[] getMergedStats(final Result result, final boolean clientsideStatsMerge) {
    if (!clientsideStatsMerge || (result.size() == 1)) {
      return result.value();
    }
    return URLClassloaderUtils.toBinary(HBaseUtils.getMergedStats(result.listCells()));
  }
}
