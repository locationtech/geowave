/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.ParallelDecoder;
import org.locationtech.geowave.datastore.hbase.HBaseRow;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Throwables;

/**
 * HBase implementation of {@link ParallelDecoder} that creates a scanner for every
 * {@link HRegionLocation} that overlaps with the query row ranges.
 *
 * @param <T> the type of the decoded rows
 */
public class HBaseParallelDecoder<T> extends ParallelDecoder<T> {

  private Filter filter;
  private TableName tableName;
  private final HBaseOperations operations;
  private final Supplier<Scan> scanProvider;
  private final List<ByteArrayRange> ranges;
  private final int partitionKeyLength;

  public HBaseParallelDecoder(
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Supplier<Scan> scanProvider,
      final HBaseOperations operations,
      final List<ByteArrayRange> ranges,
      final int partitionKeyLength) {
    super(rowTransformer);
    this.scanProvider = scanProvider;
    this.operations = operations;
    this.ranges = ranges;
    this.partitionKeyLength = partitionKeyLength;
  }

  public void setFilter(final Filter filter) {
    this.filter = filter;
  }

  public void setTableName(final TableName tableName) {
    this.tableName = tableName;
  }

  @Override
  protected List<RowProvider> getRowProviders() throws Exception {
    final List<RowProvider> scanners = Lists.newLinkedList();
    final RegionLocator locator = operations.getRegionLocator(tableName);
    final List<HRegionLocation> regionLocations = locator.getAllRegionLocations();
    Collections.shuffle(regionLocations);
    locator.close();

    if ((ranges == null) || ranges.isEmpty()) {
      // make a task for each region location
      for (final HRegionLocation regionLocation : regionLocations) {
        final HRegionInfo regionInfo = regionLocation.getRegionInfo();
        final Scan regionScan = scanProvider.get();
        regionScan.setFilter(filter);
        regionScan.setStartRow(
            regionInfo.getStartKey().length == 0 ? new byte[] {0} : regionInfo.getStartKey());
        regionScan.setStopRow(
            regionInfo.getEndKey().length == 0
                ? new byte[] {
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF}
                : regionInfo.getEndKey());
        scanners.add(
            new HBaseScanner(
                operations.getConnection(),
                tableName,
                regionScan,
                partitionKeyLength));
      }
    } else {
      // Divide all ranges into their respective regions
      // for regions with multiple ranges, create a MultiRowRangeFilter
      // create a runnable task to scan each region with ranges
      final List<Pair<byte[], byte[]>> unprocessedRanges = Lists.newLinkedList();
      for (final ByteArrayRange byteArrayRange : ranges) {
        if (byteArrayRange.getStart() != null) {
          final byte[] startRow = byteArrayRange.getStart();
          byte[] stopRow;
          if (!byteArrayRange.isSingleValue()) {
            stopRow = ByteArrayUtils.getNextPrefix(byteArrayRange.getEnd());
          } else {
            stopRow = ByteArrayUtils.getNextPrefix(byteArrayRange.getStart());
          }
          unprocessedRanges.add(new Pair<>(startRow, stopRow));
        }
      }

      for (final HRegionLocation regionLocation : regionLocations) {
        final HRegionInfo regionInfo = regionLocation.getRegionInfo();
        final List<RowRange> regionRanges = Lists.newLinkedList();
        final Iterator<Pair<byte[], byte[]>> rangeIterator = unprocessedRanges.iterator();
        while (rangeIterator.hasNext()) {
          final Pair<byte[], byte[]> byteArrayRange = rangeIterator.next();
          byte[] startRow = byteArrayRange.getFirst();
          byte[] stopRow = byteArrayRange.getSecond();
          if (((regionInfo.getEndKey().length == 0)
              || (Bytes.compareTo(startRow, regionInfo.getEndKey()) <= 0))
              && ((regionInfo.getStartKey().length == 0)
                  || (Bytes.compareTo(stopRow, regionInfo.getStartKey()) > 0))) {
            boolean partial = false;
            if (!regionInfo.containsRow(startRow)) {
              startRow = regionInfo.getStartKey();
              partial = true;
            }
            if (!regionInfo.containsRow(stopRow)) {
              stopRow = new ByteArray(regionInfo.getEndKey()).getNextPrefix();
              partial = true;
            }
            if (!partial) {
              rangeIterator.remove();
            }
            final RowRange rowRange = new RowRange(startRow, true, stopRow, false);

            regionRanges.add(rowRange);
          }
        }
        final Scan regionScan = scanProvider.get();
        if (regionRanges.size() == 1) {
          regionScan.setFilter(filter);
          regionScan.setStartRow(regionRanges.get(0).getStartRow());
          regionScan.setStopRow(regionRanges.get(0).getStopRow());
        } else if (regionRanges.size() > 1) {

          final Filter rowRangeFilter =
              new MultiRowRangeFilter(MultiRowRangeFilter.sortAndMerge(regionRanges));
          if (filter != null) {
            regionScan.setFilter(new FilterList(rowRangeFilter, filter));
          } else {
            regionScan.setFilter(rowRangeFilter);
          }
          regionScan.setStartRow(regionRanges.get(0).getStartRow());
          regionScan.setStopRow(regionRanges.get(regionRanges.size() - 1).getStopRow());

        } else {
          continue;
        }
        scanners.add(
            new HBaseScanner(
                operations.getConnection(),
                tableName,
                regionScan,
                partitionKeyLength));
      }
    }
    return scanners;
  }

  private static class HBaseScanner extends ParallelDecoder.RowProvider {

    private final TableName tableName;
    private final Connection connection;
    private final Scan sourceScanner;
    private final int partitionKeyLength;
    private Table table;
    private ResultScanner baseResults;
    private Iterator<Result> resultsIterator;

    public HBaseScanner(
        final Connection connection,
        final TableName tableName,
        final Scan sourceScanner,
        final int partitionKeyLength) {
      this.connection = connection;
      this.tableName = tableName;
      this.sourceScanner = sourceScanner;
      this.partitionKeyLength = partitionKeyLength;
    }

    @Override
    public void close() throws IOException {
      table.close();
      baseResults.close();
    }

    @Override
    public boolean hasNext() {
      return resultsIterator.hasNext();
    }

    @Override
    public GeoWaveRow next() {
      return new HBaseRow(resultsIterator.next(), partitionKeyLength);
    }

    @Override
    public void init() {
      try {
        table = connection.getTable(tableName);
        baseResults = table.getScanner(sourceScanner);
        resultsIterator = baseResults.iterator();
      } catch (final IOException e) {
        Throwables.propagate(e);
      }
    }
  }
}
