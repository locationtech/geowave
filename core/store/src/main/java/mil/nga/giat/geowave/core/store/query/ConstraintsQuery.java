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
package mil.nga.giat.geowave.core.store.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class ConstraintsQuery
{
	public static final int MAX_RANGE_DECOMPOSITION = 2000;
	public static final int AGGREGATION_RANGE_DECOMPOSITION = 10;

	public final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
	public final List<MultiDimensionalNumericData> constraints;
	public final List<DistributableQueryFilter> distributableFilters;

	private final IndexMetaData[] indexMetaData;
	private final PrimaryIndex index;

	public ConstraintsQuery(
			final List<MultiDimensionalNumericData> constraints,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final IndexMetaData[] indexMetaData,
			final PrimaryIndex index,
			final List<QueryFilter> queryFilters,
			DedupeFilter clientDedupeFilter,
			final DuplicateEntryCount duplicateCounts,
			final FilteredIndexQuery parentQuery ) {
		this.constraints = constraints;
		this.aggregation = aggregation;
		this.indexMetaData = indexMetaData != null ? indexMetaData : new IndexMetaData[] {};
		this.index = index;
		final SplitFilterLists lists = splitList(queryFilters);
		final List<QueryFilter> clientFilters = lists.clientFilters;
		if ((duplicateCounts != null) && !duplicateCounts.isAnyEntryHaveDuplicates()) {
			clientDedupeFilter = null;
		}
		// add dedupe filters to the front of both lists so that the
		// de-duplication is performed before any more complex filtering
		// operations, use the supplied client dedupe filter if possible
		if (clientDedupeFilter != null) {
			clientFilters.add(
					0,
					clientDedupeFilter);
		}
		parentQuery.setClientFilters(clientFilters);
		distributableFilters = lists.distributableFilters;
		if (!distributableFilters.isEmpty() && clientDedupeFilter != null) {
			distributableFilters.add(
					0,
					clientDedupeFilter);
		}
	}

	public boolean isAggregation() {
		return ((aggregation != null) && (aggregation.getRight() != null));
	}

	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		if ((constraints == null) || constraints.isEmpty()) {
			return new ArrayList<MultiDimensionalCoordinateRangesArray>();
		}
		else {
			final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
			final List<MultiDimensionalCoordinateRangesArray> ranges = new ArrayList<MultiDimensionalCoordinateRangesArray>();
			for (final MultiDimensionalNumericData nd : constraints) {
				ranges.add(new MultiDimensionalCoordinateRangesArray(
						indexStrategy.getCoordinateRangesPerDimension(
								nd,
								indexMetaData)));
			}
			return ranges;
		}
	}

	public List<ByteArrayRange> getRanges() {
		if (isAggregation()) {
			final List<ByteArrayRange> ranges = DataStoreUtils.constraintsToByteArrayRanges(
					constraints,
					index.getIndexStrategy(),
					AGGREGATION_RANGE_DECOMPOSITION,
					indexMetaData);
			if ((ranges == null) || (ranges.size() < 2)) {
				return ranges;
			}

			final List<ByteArrayRange> retVal = new ArrayList<ByteArrayRange>();
			retVal.add(getSingleRange(ranges));
			return retVal;
		}
		else {
			return getAllRanges();
		}
	}

	private ByteArrayRange getSingleRange(
			List<ByteArrayRange> ranges ) {
		ByteArrayId start = null;
		ByteArrayId end = null;

		for (final ByteArrayRange range : ranges) {
			if ((start == null) || (range.getStart().compareTo(
					start) < 0)) {
				start = range.getStart();
			}
			if ((end == null) || (range.getEnd().compareTo(
					end) > 0)) {
				end = range.getEnd();
			}
		}
		return new ByteArrayRange(
				start,
				end);
	}

	public List<ByteArrayRange> getAllRanges() {
		return DataStoreUtils.constraintsToByteArrayRanges(
				constraints,
				index.getIndexStrategy(),
				MAX_RANGE_DECOMPOSITION,
				indexMetaData);
	}

	private SplitFilterLists splitList(
			final List<QueryFilter> allFilters ) {
		final List<DistributableQueryFilter> distributableFilters = new ArrayList<DistributableQueryFilter>();
		final List<QueryFilter> clientFilters = new ArrayList<QueryFilter>();
		if ((allFilters == null) || allFilters.isEmpty()) {
			return new SplitFilterLists(
					distributableFilters,
					clientFilters);
		}
		for (final QueryFilter filter : allFilters) {
			if (filter instanceof DistributableQueryFilter) {
				distributableFilters.add((DistributableQueryFilter) filter);
			}
			else {
				clientFilters.add(filter);
			}
		}
		return new SplitFilterLists(
				distributableFilters,
				clientFilters);
	}

	private static class SplitFilterLists
	{
		private final List<DistributableQueryFilter> distributableFilters;
		private final List<QueryFilter> clientFilters;

		public SplitFilterLists(
				final List<DistributableQueryFilter> distributableFilters,
				final List<QueryFilter> clientFilters ) {
			this.distributableFilters = distributableFilters;
			this.clientFilters = clientFilters;
		}
	}

}
