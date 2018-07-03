/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.TransientAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.operations.HBaseOperations;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.IntermediateSplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;
import mil.nga.giat.geowave.mapreduce.splits.SplitInfo;
import mil.nga.giat.geowave.mapreduce.splits.SplitsProvider;

public class HBaseSplitsProvider extends
		SplitsProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseSplitsProvider.class);

	@Override
	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			final TreeSet<IntermediateSplitInfo> splits,
			final DataStoreOperations operations,
			final PrimaryIndex index,
			final List<Short> adapterIds,
			final Map<PrimaryIndex, RowRangeHistogramStatistics<?>> statsCache,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Integer maxSplits,
			final DistributableQuery query,
			final String[] authorizations )
			throws IOException {

		HBaseOperations hbaseOperations = null;
		if (operations instanceof HBaseOperations) {
			hbaseOperations = (HBaseOperations) operations;
		}
		else {
			LOGGER.error("HBaseSplitsProvider requires BasicHBaseOperations object.");
			return splits;
		}

		if ((query != null) && !query.isSupported(index)) {
			return splits;
		}

		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
		final int partitionKeyLength = indexStrategy.getPartitionKeyLength();

		final String tableName = hbaseOperations.getQualifiedTableName(index.getId().getString());

		// Build list of row ranges from query
		List<ByteArrayRange> ranges = null;
		if (query != null) {
			final List<MultiDimensionalNumericData> indexConstraints = query.getIndexConstraints(index);
			if ((maxSplits != null) && (maxSplits > 0)) {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						maxSplits).getCompositeQueryRanges();
			}
			else {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						-1).getCompositeQueryRanges();
			}
		}

		final Map<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> binnedRanges = new HashMap<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>>();
		final RegionLocator regionLocator = hbaseOperations.getRegionLocator(tableName);

		if (regionLocator == null) {
			LOGGER.error("Unable to retrieve RegionLocator for " + tableName);
			return splits;
		}

		RowRangeHistogramStatistics<?> stats = getHistStats(
				index,
				adapterIds,
				adapterStore,
				statsStore,
				statsCache,
				authorizations);

		if (ranges == null) { // get partition ranges from stats
			if (stats != null) {
				ranges = new ArrayList();

				ByteArrayId prevKey = new ByteArrayId(
						HConstants.EMPTY_BYTE_ARRAY);

				for (ByteArrayId partitionKey : stats.getPartitionKeys()) {
					ByteArrayRange range = new ByteArrayRange(
							prevKey,
							partitionKey);

					ranges.add(range);

					prevKey = partitionKey;
				}

				ranges.add(new ByteArrayRange(
						prevKey,
						new ByteArrayId(
								HConstants.EMPTY_BYTE_ARRAY)));

				binRanges(
						ranges,
						binnedRanges,
						regionLocator);
			}
			else {
				binFullRange(
						binnedRanges,
						regionLocator);
			}

		}
		else {
			while (!ranges.isEmpty()) {
				ranges = binRanges(
						ranges,
						binnedRanges,
						regionLocator);
			}
		}

		for (final Entry<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> locationEntry : binnedRanges
				.entrySet()) {
			final String hostname = locationEntry.getKey().getHostname();

			for (final Entry<HRegionInfo, List<ByteArrayRange>> regionEntry : locationEntry.getValue().entrySet()) {
				final Map<ByteArrayId, SplitInfo> splitInfo = new HashMap<ByteArrayId, SplitInfo>();
				final List<RangeLocationPair> rangeList = new ArrayList<RangeLocationPair>();

				for (final ByteArrayRange range : regionEntry.getValue()) {
					GeoWaveRowRange gwRange = fromHBaseRange(
							range,
							partitionKeyLength);

					final double cardinality = getCardinality(
							getHistStats(
									index,
									adapterIds,
									adapterStore,
									statsStore,
									statsCache,
									authorizations),
							gwRange,
							index.getIndexStrategy().getPartitionKeyLength());

					rangeList.add(new RangeLocationPair(
							gwRange,
							hostname,
							cardinality < 1 ? 1.0 : cardinality));
				}

				if (!rangeList.isEmpty()) {
					splitInfo.put(
							index.getId(),
							new SplitInfo(
									index,
									rangeList));
					splits.add(new IntermediateSplitInfo(
							splitInfo,
							this));
				}
			}
		}

		return splits;
	}

	protected static void binFullRange(
			final Map<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> binnedRanges,
			final RegionLocator regionLocator )
			throws IOException {

		List<HRegionLocation> locations = regionLocator.getAllRegionLocations();

		for (HRegionLocation location : locations) {
			Map<HRegionInfo, List<ByteArrayRange>> regionInfoMap = binnedRanges.get(location);
			if (regionInfoMap == null) {
				regionInfoMap = new HashMap<HRegionInfo, List<ByteArrayRange>>();
				binnedRanges.put(
						location,
						regionInfoMap);
			}

			final HRegionInfo regionInfo = location.getRegionInfo();
			List<ByteArrayRange> rangeList = regionInfoMap.get(regionInfo);
			if (rangeList == null) {
				rangeList = new ArrayList<ByteArrayRange>();
				regionInfoMap.put(
						regionInfo,
						rangeList);
			}

			final ByteArrayRange regionRange = new ByteArrayRange(
					new ByteArrayId(
							regionInfo.getStartKey()),
					new ByteArrayId(
							regionInfo.getEndKey()));
			rangeList.add(regionRange);
		}
	}

	protected static List<ByteArrayRange> binRanges(
			final List<ByteArrayRange> inputRanges,
			final Map<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> binnedRanges,
			final RegionLocator regionLocator )
			throws IOException {

		// Loop through ranges, getting RegionLocation and RegionInfo for
		// startKey, clipping range by that regionInfo's extent, and leaving
		// remainder in the List to be region'd
		final ListIterator<ByteArrayRange> i = inputRanges.listIterator();
		while (i.hasNext()) {
			final ByteArrayRange range = i.next();
			byte[] startKey = range == null ? HConstants.EMPTY_BYTE_ARRAY : range.getStart().getBytes();
			byte[] endKey = range == null ? HConstants.EMPTY_BYTE_ARRAY : range.getEnd().getBytes();

			final HRegionLocation location = regionLocator.getRegionLocation(startKey);

			Map<HRegionInfo, List<ByteArrayRange>> regionInfoMap = binnedRanges.get(location);
			if (regionInfoMap == null) {
				regionInfoMap = new HashMap<HRegionInfo, List<ByteArrayRange>>();
				binnedRanges.put(
						location,
						regionInfoMap);
			}

			final HRegionInfo regionInfo = location.getRegionInfo();
			List<ByteArrayRange> rangeList = regionInfoMap.get(regionInfo);
			if (rangeList == null) {
				rangeList = new ArrayList<ByteArrayRange>();
				regionInfoMap.put(
						regionInfo,
						rangeList);
			}

			// Check if region contains range or if it's the last range
			if ((endKey == HConstants.EMPTY_BYTE_ARRAY) || regionInfo.containsRange(
					startKey,
					endKey)) {
				rangeList.add(range);
				i.remove();
			}
			else {
				ByteArrayRange thisRange = new ByteArrayRange(
						new ByteArrayId(
								startKey),
						new ByteArrayId(
								endKey));
				ByteArrayRange regionRange = new ByteArrayRange(
						new ByteArrayId(
								regionInfo.getStartKey()),
						new ByteArrayId(
								regionInfo.getEndKey()));

				final ByteArrayRange overlappingRange = thisRange.intersection(regionRange);

				rangeList.add(new ByteArrayRange(
						overlappingRange.getStart(),
						overlappingRange.getEnd()));
				i.remove();

				i.add(new ByteArrayRange(
						new ByteArrayId(
								regionInfo.getEndKey()),
						new ByteArrayId(
								endKey)));
			}
		}
		// the underlying assumption is that by the end of this any input range
		// at least has the partition key portion and is the same partition key
		// for start and end keys on the range, because thats really by
		// definition what a region or tablets is using split points
		return inputRanges;
	}

	protected static GeoWaveRowRange rangeIntersection(
			final GeoWaveRowRange thisRange,
			final GeoWaveRowRange otherRange ) {
		ByteArrayRange thisByteArrayRange = new ByteArrayRange(
				new ByteArrayId(
						thisRange.getStartSortKey()),
				new ByteArrayId(
						thisRange.getEndSortKey()));
		ByteArrayRange otherByteArrayRange = new ByteArrayRange(
				new ByteArrayId(
						otherRange.getStartSortKey()),
				new ByteArrayId(
						otherRange.getEndSortKey()));

		final ByteArrayRange overlappingRange = thisByteArrayRange.intersection(otherByteArrayRange);

		return new GeoWaveRowRange(
				null,
				overlappingRange.getStart().getBytes(),
				overlappingRange.getEnd().getBytes(),
				true,
				false);
	}

	public static ByteArrayRange toHBaseRange(
			final GeoWaveRowRange range ) {

		if ((range.getPartitionKey() == null) || (range.getPartitionKey().length == 0)) {
			byte[] startKey = (range.getStartSortKey() == null) ? HConstants.EMPTY_BYTE_ARRAY : range.getStartSortKey();
			byte[] endKey = (range.getEndSortKey() == null) ? HConstants.EMPTY_BYTE_ARRAY : range.getEndSortKey();

			return new ByteArrayRange(
					new ByteArrayId(
							startKey),
					new ByteArrayId(
							endKey));
		}
		else {
			byte[] startKey = (range.getStartSortKey() == null) ? range.getPartitionKey() : ArrayUtils.addAll(
					range.getPartitionKey(),
					range.getStartSortKey());

			byte[] endKey = (range.getEndSortKey() == null) ? ByteArrayId.getNextPrefix(range.getPartitionKey())
					: ArrayUtils.addAll(
							range.getPartitionKey(),
							range.getEndSortKey());

			return new ByteArrayRange(
					new ByteArrayId(
							startKey),
					new ByteArrayId(
							endKey));
		}
	}

	public static GeoWaveRowRange fromHBaseRange(
			final ByteArrayRange range,
			final int partitionKeyLength ) {
		byte[] startRow = Bytes.equals(
				range.getStart().getBytes(),
				HConstants.EMPTY_BYTE_ARRAY) ? null : range.getStart().getBytes();

		byte[] stopRow = Bytes.equals(
				range.getEnd().getBytes(),
				HConstants.EMPTY_BYTE_ARRAY) ? null : range.getEnd().getBytes();

		if (partitionKeyLength <= 0) {
			return new GeoWaveRowRange(
					null,
					startRow,
					stopRow,
					true,
					false);
		}
		else {
			byte[] partitionKey;
			boolean partitionKeyDiffers = false;
			if ((startRow == null) && (stopRow == null)) {
				return new GeoWaveRowRange(
						null,
						null,
						null,
						true,
						true);
			}
			else if (startRow != null) {
				partitionKey = ArrayUtils.subarray(
						startRow,
						0,
						partitionKeyLength);
				if (stopRow != null) {
					partitionKeyDiffers = !Arrays.equals(
							partitionKey,
							ArrayUtils.subarray(
									stopRow,
									0,
									partitionKeyLength));
				}
			}
			else {
				partitionKey = ArrayUtils.subarray(
						stopRow,
						0,
						partitionKeyLength);
			}
			return new GeoWaveRowRange(
					partitionKey,
					startRow == null ? null : (partitionKeyLength == startRow.length ? null : ArrayUtils.subarray(
							startRow,
							partitionKeyLength,
							startRow.length)),
					partitionKeyDiffers ? null : (stopRow == null ? null : (partitionKeyLength == stopRow.length ? null
							: ArrayUtils.subarray(
									stopRow,
									partitionKeyLength,
									stopRow.length))),
					true,
					partitionKeyDiffers);

		}
	}

}
