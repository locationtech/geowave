/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.ParallelDecoder;
import org.locationtech.geowave.datastore.hbase.HBaseRow;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Throwables;
import com.google.inject.Provider;

/**
 * HBase implementation of {@link ParallelDecoder} that creates a scanner for
 * every {@link HRegionLocation} that overlaps with the query row ranges.
 *
 * @param <T>
 *            the type of the decoded rows
 */
public class HBaseParallelDecoder<T> extends
		ParallelDecoder<T>
{

	private Filter filter;
	private TableName tableName;
	private final HBaseOperations operations;
	private final Provider<Scan> scanProvider;
	private final List<ByteArrayRange> ranges;
	private final int partitionKeyLength;

	public HBaseParallelDecoder(
			GeoWaveRowIteratorTransformer<T> rowTransformer,
			Provider<Scan> scanProvider,
			HBaseOperations operations,
			List<ByteArrayRange> ranges,
			int partitionKeyLength ) {
		super(
				rowTransformer);
		this.scanProvider = scanProvider;
		this.operations = operations;
		this.ranges = ranges;
		this.partitionKeyLength = partitionKeyLength;
	}

	public void setFilter(
			Filter filter ) {
		this.filter = filter;
	}

	public void setTableName(
			TableName tableName ) {
		this.tableName = tableName;
	}

	@Override
	protected List<RowProvider> getRowProviders()
			throws Exception {
		List<RowProvider> scanners = Lists.newLinkedList();
		RegionLocator locator = operations.getRegionLocator(tableName);
		List<HRegionLocation> regionLocations = locator.getAllRegionLocations();
		Collections.shuffle(regionLocations);
		locator.close();

		if (ranges == null || ranges.isEmpty()) {
			// make a task for each region location
			for (HRegionLocation regionLocation : regionLocations) {
				HRegionInfo regionInfo = regionLocation.getRegionInfo();
				Scan regionScan = scanProvider.get();
				regionScan.setFilter(filter);
				regionScan.setStartRow(regionInfo.getStartKey().length == 0 ? new byte[] {
					0
				} : regionInfo.getStartKey());
				regionScan.setStopRow(regionInfo.getEndKey().length == 0 ? new byte[] {
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF,
					(byte) 0xFF
				} : regionInfo.getEndKey());
				scanners.add(new HBaseScanner(
						operations.getConnection(),
						tableName,
						regionScan,
						partitionKeyLength));
			}
		}
		else {
			// Divide all ranges into their respective regions
			// for regions with multiple ranges, create a MultiRowRangeFilter
			// create a runnable task to scan each region with ranges
			List<Pair<byte[], byte[]>> unprocessedRanges = Lists.newLinkedList();
			for (ByteArrayRange byteArrayRange : ranges) {
				if (byteArrayRange.getStart() != null) {
					byte[] startRow = byteArrayRange.getStart().getBytes();
					byte[] stopRow;
					if (!byteArrayRange.isSingleValue()) {
						stopRow = byteArrayRange.getEnd().getNextPrefix();
					}
					else {
						stopRow = byteArrayRange.getStart().getNextPrefix();
					}
					unprocessedRanges.add(new Pair<byte[], byte[]>(
							startRow,
							stopRow));
				}
			}

			for (HRegionLocation regionLocation : regionLocations) {
				HRegionInfo regionInfo = regionLocation.getRegionInfo();
				List<RowRange> regionRanges = Lists.newLinkedList();
				Iterator<Pair<byte[], byte[]>> rangeIterator = unprocessedRanges.iterator();
				while (rangeIterator.hasNext()) {
					Pair<byte[], byte[]> byteArrayRange = rangeIterator.next();
					byte[] startRow = byteArrayRange.getFirst();
					byte[] stopRow = byteArrayRange.getSecond();
					if ((regionInfo.getEndKey().length == 0 || Bytes.compareTo(
							startRow,
							regionInfo.getEndKey()) <= 0) && (regionInfo.getStartKey().length == 0 || Bytes.compareTo(
							stopRow,
							regionInfo.getStartKey()) > 0)) {
						boolean partial = false;
						if (!regionInfo.containsRow(startRow)) {
							startRow = regionInfo.getStartKey();
							partial = true;
						}
						if (!regionInfo.containsRow(stopRow)) {
							stopRow = new ByteArray(
									regionInfo.getEndKey()).getNextPrefix();
							partial = true;
						}
						if (!partial) {
							rangeIterator.remove();
						}
						final RowRange rowRange = new RowRange(
								startRow,
								true,
								stopRow,
								false);

						regionRanges.add(rowRange);

					}
				}
				Scan regionScan = scanProvider.get();
				if (regionRanges.size() == 1) {
					regionScan.setFilter(filter);
					regionScan.setStartRow(regionRanges.get(
							0).getStartRow());
					regionScan.setStopRow(regionRanges.get(
							0).getStopRow());
				}
				else if (regionRanges.size() > 1) {

					Filter rowRangeFilter = new MultiRowRangeFilter(
							MultiRowRangeFilter.sortAndMerge(regionRanges));
					if (filter != null) {
						regionScan.setFilter(new FilterList(
								rowRangeFilter,
								filter));
					}
					else {
						regionScan.setFilter(rowRangeFilter);
					}
					regionScan.setStartRow(regionRanges.get(
							0).getStartRow());
					regionScan.setStopRow(regionRanges.get(
							regionRanges.size() - 1).getStopRow());

				}
				else {
					continue;
				}
				scanners.add(new HBaseScanner(
						operations.getConnection(),
						tableName,
						regionScan,
						partitionKeyLength));
			}
		}
		return scanners;
	}

	private static class HBaseScanner extends
			ParallelDecoder.RowProvider
	{

		private final TableName tableName;
		private final Connection connection;
		private final Scan sourceScanner;
		private final int partitionKeyLength;
		private Table table;
		private ResultScanner baseResults;
		private Iterator<Result> resultsIterator;

		public HBaseScanner(
				Connection connection,
				TableName tableName,
				Scan sourceScanner,
				int partitionKeyLength ) {
			this.connection = connection;
			this.tableName = tableName;
			this.sourceScanner = sourceScanner;
			this.partitionKeyLength = partitionKeyLength;
		}

		@Override
		public void close()
				throws IOException {
			table.close();
			baseResults.close();
		}

		@Override
		public boolean hasNext() {
			return resultsIterator.hasNext();
		}

		@Override
		public GeoWaveRow next() {
			return new HBaseRow(
					resultsIterator.next(),
					partitionKeyLength);
		}

		@Override
		public void init() {
			try {
				table = connection.getTable(tableName);
				baseResults = table.getScanner(sourceScanner);
				resultsIterator = baseResults.iterator();
			}
			catch (IOException e) {
				Throwables.propagate(e);
			}
		}

	}

}
