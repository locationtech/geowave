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
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.DataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.filter.DedupeFilter;
import org.locationtech.geowave.core.store.filter.DistributableFilterList;
import org.locationtech.geowave.core.store.filter.DistributableQueryFilter;
import org.locationtech.geowave.core.store.filter.QueryFilter;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.Reader;
import org.locationtech.geowave.core.store.query.CoordinateRangeQueryFilter;
import org.locationtech.geowave.core.store.query.Query;
import org.locationtech.geowave.core.store.query.aggregate.Aggregation;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;
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
	public final List<DistributableQueryFilter> distributableFilters;

	public final IndexMetaData[] indexMetaData;
	private final PrimaryIndex index;

	public BaseConstraintsQuery(
			final List<Short> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair,
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
			final List<Short> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair,
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
	public DistributableQueryFilter getServerFilter(
			final DataStoreOptions options ) {
		// TODO GEOWAVE-1018 is options necessary? is this correct?
		if ((distributableFilters == null) || distributableFilters.isEmpty()) {
			return null;
		}
		else if (distributableFilters.size() > 1) {
			return new DistributableFilterList(
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
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit,
			final Integer queryMaxRangeDecomposition,
			boolean delete ) {
		if (isAggregation()) {
			if ((options == null) || !options.isServerSideLibraryEnabled()) {
				// Aggregate client-side
				final CloseableIterator<Object> it = (CloseableIterator<Object>) super.query(
						datastoreOperations,
						options,
						adapterStore,
						maxResolutionSubsamplingPerDimension,
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
					QueryFilter f = clientFilters.get(clientFilters.size() - 1);
					if (f instanceof DedupeFilter) {
						distributableFilters.add((DedupeFilter) f);
						LOGGER
								.warn("Aggregating results when duplicates exist in the table may result in duplicate aggregation");
					}
				}
				try (final Reader<GeoWaveRow> reader = getReader(
						datastoreOperations,
						options,
						adapterStore,
						maxResolutionSubsamplingPerDimension,
						limit,
						queryMaxRangeDecomposition,
						GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
						false)) {
					Mergeable mergedAggregationResult = null;
					if ((reader == null) || !reader.hasNext()) {
						return new CloseableIterator.Empty();
					}
					else {
						while (reader.hasNext()) {
							final GeoWaveRow row = reader.next();
							for (final GeoWaveValue value : row.getFieldValues()) {
								if ((value.getValue() != null) && (value.getValue().length > 0)) {
									if (mergedAggregationResult == null) {
										mergedAggregationResult = (Mergeable) PersistenceUtils.fromBinary(value
												.getValue());
									}
									else {
										mergedAggregationResult.merge((Mergeable) PersistenceUtils.fromBinary(value
												.getValue()));
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
				maxResolutionSubsamplingPerDimension,
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

	@Override
	protected QueryRanges getRanges(
			int maxRangeDecomposition ) {
		return DataStoreUtils.constraintsToQueryRanges(
				constraints,
				index.getIndexStrategy(),
				maxRangeDecomposition,
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
