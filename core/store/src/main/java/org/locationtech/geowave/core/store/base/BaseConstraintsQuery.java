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
package org.locationtech.geowave.core.store.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.CoordinateRangeQueryFilter;
import org.locationtech.geowave.core.store.query.filter.DedupeFilter;
import org.locationtech.geowave.core.store.query.filter.FilterList;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;

import com.google.common.collect.Iterators;

/**
 * This class represents basic numeric contraints applied to a datastore query
 *
 */
public class BaseConstraintsQuery extends
		BaseFilteredIndexQuery
{

	private final static Logger LOGGER = Logger.getLogger(BaseConstraintsQuery.class);
	private boolean queryFiltersEnabled;

	public final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
	public final List<MultiDimensionalNumericData> constraints;
	public final List<QueryFilter> distributableFilters;

	public final IndexMetaData[] indexMetaData;
	private final Index index;

	public BaseConstraintsQuery(
			final short[] adapterIds,
			final Index index,
			final QueryConstraints query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<String[], InternalDataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount differingVisibilityCounts,
			final FieldVisibilityCount visibilityCounts,
			final String[] authorizations ) {
		this(
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(index) : null,
				query != null ? query.createFilters(index) : null,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				fieldIdsAdapterPair,
				indexMetaData,
				duplicateCounts,
				differingVisibilityCounts,
				visibilityCounts,
				authorizations);
	}

	public BaseConstraintsQuery(
			final short[] adapterIds,
			final Index index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<String[], InternalDataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount differingVisibilityCounts,
			final FieldVisibilityCount visibilityCounts,
			final String[] authorizations ) {
		super(
				adapterIds,
				index,
				scanCallback,
				fieldIdsAdapterPair,
				differingVisibilityCounts,
				visibilityCounts,
				authorizations);
		this.constraints = constraints;
		this.aggregation = aggregation;
		this.indexMetaData = indexMetaData != null ? indexMetaData : new IndexMetaData[] {};
		this.index = index;
		final SplitFilterLists lists = splitList(queryFilters);
		final List<QueryFilter> clientFilters = lists.clientFilters;
		if ((duplicateCounts != null) && !duplicateCounts.isAnyEntryHaveDuplicates()) {
			clientDedupeFilter = null;
		}
		distributableFilters = lists.distributableFilters;
		if (clientDedupeFilter != null) {
			clientFilters.add(clientDedupeFilter);
		}
		this.clientFilters = clientFilters;

		queryFiltersEnabled = true;
	}

	@Override
	public QueryFilter getServerFilter(
			final DataStoreOptions options ) {
		// TODO GEOWAVE-1018 is options necessary? is this correct?
		if ((distributableFilters == null) || distributableFilters.isEmpty()) {
			return null;
		}
		else if (distributableFilters.size() > 1) {
			return new FilterList(
					distributableFilters);
		}
		else {
			return distributableFilters.get(0);
		}
	}

	public boolean isQueryFiltersEnabled() {
		return queryFiltersEnabled;
	}

	public void setQueryFiltersEnabled(
			final boolean queryFiltersEnabled ) {
		this.queryFiltersEnabled = queryFiltersEnabled;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CloseableIterator<Object> query(
			final DataStoreOperations datastoreOperations,
			final DataStoreOptions options,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final double[] targetResolutionPerDimensionForHierarchicalIndex,
			final Integer limit,
			final Integer queryMaxRangeDecomposition,
			final boolean delete ) {
		if (isAggregation()) {
			if ((options == null) || !options.isServerSideLibraryEnabled()) {
				// Aggregate client-side
				final CloseableIterator<Object> it = super.query(
						datastoreOperations,
						options,
						adapterStore,
						internalAdapterStore,
						maxResolutionSubsamplingPerDimension,
						targetResolutionPerDimensionForHierarchicalIndex,
						limit,
						queryMaxRangeDecomposition,
						false);
				return BaseDataStoreUtils.aggregate(
						it,
						(Aggregation<?, ?, Object>) aggregation.getValue());
			}
			else {
				// the aggregation is run server-side use the reader to
				// aggregate to a single value here

				// should see if there is a client dedupe filter thats been
				// added and run it serverside
				// also if so and duplicates cross partitions, the dedupe filter
				// still won't be effective and the aggregation will return
				// incorrect results
				if (!clientFilters.isEmpty()) {
					final QueryFilter f = clientFilters.get(clientFilters.size() - 1);
					if (f instanceof DedupeFilter) {
						distributableFilters.add(f);
						LOGGER
								.warn("Aggregating results when duplicates exist in the table may result in duplicate aggregation");
					}
				}
				try (final RowReader<GeoWaveRow> reader = getReader(
						datastoreOperations,
						options,
						adapterStore,
						internalAdapterStore,
						maxResolutionSubsamplingPerDimension,
						targetResolutionPerDimensionForHierarchicalIndex,
						limit,
						queryMaxRangeDecomposition,
						GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
						false)) {
					Object mergedAggregationResult = null;
					Aggregation<?, Object, Object> agg = (Aggregation<?, Object, Object>) aggregation.getValue();
					if ((reader == null) || !reader.hasNext()) {
						return new CloseableIterator.Empty();
					}
					else {
						while (reader.hasNext()) {
							final GeoWaveRow row = reader.next();
							for (final GeoWaveValue value : row.getFieldValues()) {
								if ((value.getValue() != null) && (value.getValue().length > 0)) {
									if (mergedAggregationResult == null) {
										mergedAggregationResult = agg.resultFromBinary(value.getValue());
									}
									else {
										mergedAggregationResult = agg.merge(
												mergedAggregationResult,
												agg.resultFromBinary(value.getValue()));
									}
								}
							}
						}
						return new CloseableIterator.Wrapper<>(
								Iterators.singletonIterator(mergedAggregationResult));
					}
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to close reader for aggregation",
							e);
				}
			}
		}
		return super.query(
				datastoreOperations,
				options,
				adapterStore,
				internalAdapterStore,
				maxResolutionSubsamplingPerDimension,
				targetResolutionPerDimensionForHierarchicalIndex,
				limit,
				queryMaxRangeDecomposition,
				delete);
	}

	@Override
	protected List<QueryFilter> getClientFiltersList(
			final DataStoreOptions options ) {

		// Since we have custom filters enabled, this list should only return
		// the client filters
		if ((options != null) && options.isServerSideLibraryEnabled()) {
			return clientFilters;
		}
		// add a index filter to the front of the list if there isn't already a
		// filter
		if (distributableFilters.isEmpty()
				|| ((distributableFilters.size() == 1) && (distributableFilters.get(0) instanceof DedupeFilter))) {
			final List<MultiDimensionalCoordinateRangesArray> coords = getCoordinateRanges();
			if (!coords.isEmpty()) {
				clientFilters.add(
						0,
						new CoordinateRangeQueryFilter(
								index.getIndexStrategy(),
								coords.toArray(new MultiDimensionalCoordinateRangesArray[] {})));
			}
		}
		else {
			// Without custom filters, we need all the filters on the client
			// side
			for (final QueryFilter distributable : distributableFilters) {
				if (!clientFilters.contains(distributable)) {
					clientFilters.add(distributable);
				}
			}
		}
		return clientFilters;
	}

	@Override
	protected boolean isCommonIndexAggregation() {
		return isAggregation() && (aggregation.getRight() instanceof CommonIndexAggregation);
	}

	@Override
	protected Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregation;
	}

	@Override
	public List<MultiDimensionalNumericData> getConstraints() {
		return constraints;
	}

	@Override
	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		if ((constraints == null) || constraints.isEmpty()) {
			return new ArrayList<>();
		}
		else {
			final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
			final List<MultiDimensionalCoordinateRangesArray> ranges = new ArrayList<>();
			for (final MultiDimensionalNumericData nd : constraints) {
				ranges.add(new MultiDimensionalCoordinateRangesArray(
						indexStrategy.getCoordinateRangesPerDimension(
								nd,
								indexMetaData)));
			}
			return ranges;
		}
	}

	@Override
	protected QueryRanges getRanges(
			final int maxRangeDecomposition,
			final double[] targetResolutionPerDimensionForHierarchicalIndex ) {
		return DataStoreUtils.constraintsToQueryRanges(
				constraints,
				index.getIndexStrategy(),
				targetResolutionPerDimensionForHierarchicalIndex,
				maxRangeDecomposition,
				indexMetaData);
	}

	private SplitFilterLists splitList(
			final List<QueryFilter> allFilters ) {
		final List<QueryFilter> distributableFilters = new ArrayList<>();
		final List<QueryFilter> clientFilters = new ArrayList<>();
		if ((allFilters == null) || allFilters.isEmpty()) {
			return new SplitFilterLists(
					distributableFilters,
					clientFilters);
		}
		for (final QueryFilter filter : allFilters) {
			if (filter instanceof QueryFilter) {
				distributableFilters.add(filter);
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
		private final List<QueryFilter> distributableFilters;
		private final List<QueryFilter> clientFilters;

		public SplitFilterLists(
				final List<QueryFilter> distributableFilters,
				final List<QueryFilter> clientFilters ) {
			this.distributableFilters = distributableFilters;
			this.clientFilters = clientFilters;
		}
	}
}
