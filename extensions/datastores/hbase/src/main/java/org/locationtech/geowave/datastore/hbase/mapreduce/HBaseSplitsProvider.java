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
package org.locationtech.geowave.datastore.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.PartitionStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.hbase.operations.HBaseOperations;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.IntermediateSplitInfo;
import org.locationtech.geowave.mapreduce.splits.RangeLocationPair;
import org.locationtech.geowave.mapreduce.splits.SplitInfo;
import org.locationtech.geowave.mapreduce.splits.SplitsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSplitsProvider extends
		SplitsProvider
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseSplitsProvider.class);

	@Override
	protected TreeSet<IntermediateSplitInfo> populateIntermediateSplits(
			final TreeSet<IntermediateSplitInfo> splits,
			final DataStoreOperations operations,
			final Index index,
			final List<Short> adapterIds,
			final Map<Pair<Index, ByteArray>, RowRangeHistogramStatistics<?>> statsCache,
			final TransientAdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final Integer maxSplits,
			final QueryConstraints query,
			final double[] targetResolutionPerDimensionForHierarchicalIndex,
			final IndexMetaData[] indexMetadata,
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

		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
		final int partitionKeyLength = indexStrategy.getPartitionKeyLength();

		final String tableName = hbaseOperations.getQualifiedTableName(index.getName());

		final Map<HRegionLocation, Map<HRegionInfo, List<ByteArrayRange>>> binnedRanges = new HashMap<>();
		final RegionLocator regionLocator = hbaseOperations.getRegionLocator(tableName);

		if (regionLocator == null) {
			LOGGER.error("Unable to retrieve RegionLocator for " + tableName);
			return splits;
		}
		// Build list of row ranges from query
		List<ByteArrayRange> ranges = null;
		if (query != null) {
			final List<MultiDimensionalNumericData> indexConstraints = query.getIndexConstraints(index);
			if ((maxSplits != null) && (maxSplits > 0)) {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						targetResolutionPerDimensionForHierarchicalIndex,
						maxSplits,
						indexMetadata).getCompositeQueryRanges();
			}
			else {
				ranges = DataStoreUtils.constraintsToQueryRanges(
						indexConstraints,
						indexStrategy,
						targetResolutionPerDimensionForHierarchicalIndex,
						-1,
						indexMetadata).getCompositeQueryRanges();
			}
		}

		if (ranges == null) { // get partition ranges from stats
			final PartitionStatistics<?> statistics = getPartitionStats(
					index,
					adapterIds,
					statsStore,
					authorizations);
			if (statistics != null) {
				ranges = new ArrayList();

				ByteArray prevKey = new ByteArray(
						HConstants.EMPTY_BYTE_ARRAY);
				final TreeSet<ByteArray> sortedPartitions = new TreeSet<>(
						statistics.getPartitionKeys());
				for (final ByteArray partitionKey : sortedPartitions) {
					final ByteArrayRange range = new ByteArrayRange(
							prevKey,
							partitionKey);

					ranges.add(range);

					prevKey = partitionKey;
				}

				ranges.add(new ByteArrayRange(
						prevKey,
						new ByteArray(
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
				final Map<String, SplitInfo> splitInfo = new HashMap<>();
				final List<RangeLocationPair> rangeList = new ArrayList<>();

				for (final ByteArrayRange range : regionEntry.getValue()) {
					final GeoWaveRowRange gwRange = toRowRange(
							range,
							partitionKeyLength);

					final double cardinality = getCardinality(
							getHistStats(
									index,
									adapterIds,
									adapterStore,
									statsStore,
									statsCache,
									new ByteArray(
											gwRange.getPartitionKey()),
									authorizations),
							gwRange);

					rangeList.add(new RangeLocationPair(
							gwRange,
							hostname,
							cardinality < 1 ? 1.0 : cardinality));
				}

				if (!rangeList.isEmpty()) {
					splitInfo.put(
							index.getName(),
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

		final List<HRegionLocation> locations = regionLocator.getAllRegionLocations();

		for (final HRegionLocation location : locations) {
			Map<HRegionInfo, List<ByteArrayRange>> regionInfoMap = binnedRanges.get(location);
			if (regionInfoMap == null) {
				regionInfoMap = new HashMap<>();
				binnedRanges.put(
						location,
						regionInfoMap);
			}

			final HRegionInfo regionInfo = location.getRegionInfo();
			List<ByteArrayRange> rangeList = regionInfoMap.get(regionInfo);
			if (rangeList == null) {
				rangeList = new ArrayList<>();
				regionInfoMap.put(
						regionInfo,
						rangeList);
			}

			final ByteArrayRange regionRange = new ByteArrayRange(
					new ByteArray(
							regionInfo.getStartKey()),
					new ByteArray(
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
			final byte[] startKey = range == null ? HConstants.EMPTY_BYTE_ARRAY : range.getStart().getBytes();
			final byte[] endKey = range == null ? HConstants.EMPTY_BYTE_ARRAY : range.getEnd().getBytes();

			final HRegionLocation location = regionLocator.getRegionLocation(startKey);

			Map<HRegionInfo, List<ByteArrayRange>> regionInfoMap = binnedRanges.get(location);
			if (regionInfoMap == null) {
				regionInfoMap = new HashMap<>();
				binnedRanges.put(
						location,
						regionInfoMap);
			}

			final HRegionInfo regionInfo = location.getRegionInfo();
			List<ByteArrayRange> rangeList = regionInfoMap.get(regionInfo);
			if (rangeList == null) {
				rangeList = new ArrayList<>();
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
				final ByteArrayRange thisRange = new ByteArrayRange(
						new ByteArray(
								startKey),
						new ByteArray(
								endKey));
				final ByteArrayRange regionRange = new ByteArrayRange(
						new ByteArray(
								regionInfo.getStartKey()),
						new ByteArray(
								regionInfo.getEndKey()));

				final ByteArrayRange overlappingRange = thisRange.intersection(regionRange);

				rangeList.add(new ByteArrayRange(
						overlappingRange.getStart(),
						overlappingRange.getEnd()));
				i.remove();

				i.add(new ByteArrayRange(
						new ByteArray(
								regionInfo.getEndKey()),
						new ByteArray(
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
		final ByteArrayRange thisByteArrayRange = new ByteArrayRange(
				new ByteArray(
						thisRange.getStartSortKey()),
				new ByteArray(
						thisRange.getEndSortKey()));
		final ByteArrayRange otherByteArrayRange = new ByteArrayRange(
				new ByteArray(
						otherRange.getStartSortKey()),
				new ByteArray(
						otherRange.getEndSortKey()));

		final ByteArrayRange overlappingRange = thisByteArrayRange.intersection(otherByteArrayRange);

		return new GeoWaveRowRange(
				null,
				overlappingRange.getStart().getBytes(),
				overlappingRange.getEnd().getBytes(),
				true,
				false);
	}
}
