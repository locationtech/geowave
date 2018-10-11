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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.DataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.callback.ScanCallbackList;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.filter.FilterList;
import org.locationtech.geowave.core.store.filter.QueryFilter;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.Reader;
import org.locationtech.geowave.core.store.operations.ReaderClosableWrapper;
import org.locationtech.geowave.core.store.util.MergingEntryIterator;
import org.locationtech.geowave.core.store.util.NativeEntryIteratorWrapper;

import com.google.common.collect.Iterators;

abstract class BaseFilteredIndexQuery extends
		BaseQuery
{
	protected List<QueryFilter> clientFilters;
	private final static Logger LOGGER = Logger.getLogger(BaseFilteredIndexQuery.class);

	public BaseFilteredIndexQuery(
			final List<Short> adapterIds,
			final PrimaryIndex index,
			final ScanCallback<?, ?> scanCallback,
			final Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount differingVisibilityCounts,
			final FieldVisibilityCount visibilityCounts,
			final String... authorizations ) {
		super(
				adapterIds,
				index,
				fieldIdsAdapterPair,
				scanCallback,
				differingVisibilityCounts,
				visibilityCounts,
				authorizations);
	}

	protected List<QueryFilter> getClientFilters() {
		return clientFilters;
	}

	@SuppressWarnings({
		"unchecked",
		"rawtypes"
	})
	public CloseableIterator<Object> query(
			final DataStoreOperations datastoreOperations,
			final DataStoreOptions options,
			final PersistentAdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit,
			final Integer queryMaxRangeDecomposition,
			boolean delete ) {
		final Reader<?> reader = getReader(
				datastoreOperations,
				options,
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit,
				queryMaxRangeDecomposition,
				getRowTransformer(
						options,
						adapterStore,
						maxResolutionSubsamplingPerDimension,
						!isCommonIndexAggregation()),
				delete);
		if (reader == null) {
			return new CloseableIterator.Empty();
		}
		Iterator it = reader;
		if ((limit != null) && (limit > 0)) {
			it = Iterators.limit(
					it,
					limit);
		}
		return new CloseableIteratorWrapper(
				new ReaderClosableWrapper(
						reader),
				it);
	}

	@Override
	protected <C> Reader<C> getReader(
			final DataStoreOperations datastoreOperations,
			final DataStoreOptions options,
			final PersistentAdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit,
			final Integer queryMaxRangeDecomposition,
			final GeoWaveRowIteratorTransformer<C> rowTransformer,
			boolean delete ) {
		boolean exists = false;
		try {
			exists = datastoreOperations.indexExists(index.getId());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Table does not exist",
					e);
		}
		if (!exists) {
			LOGGER.warn("Table does not exist " + StringUtils.stringFromBinary(index.getId().getBytes()));
			return null;
		}

		return super.getReader(
				datastoreOperations,
				options,
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit,
				queryMaxRangeDecomposition,
				rowTransformer,
				delete);
	}

	protected Map<Short, RowMergingDataAdapter> getMergingAdapters(
			final PersistentAdapterStore adapterStore ) {
		final Map<Short, RowMergingDataAdapter> mergingAdapters = new HashMap<Short, RowMergingDataAdapter>();
		for (final Short adapterId : adapterIds) {
			final DataAdapter<?> adapter = adapterStore.getAdapter(
					adapterId).getAdapter();
			if ((adapter instanceof RowMergingDataAdapter)
					&& (((RowMergingDataAdapter) adapter).getTransform() != null)) {
				mergingAdapters.put(
						adapterId,
						(RowMergingDataAdapter) adapter);
			}
		}

		return mergingAdapters;
	}

	private <T> GeoWaveRowIteratorTransformer<T> getRowTransformer(
			final DataStoreOptions options,
			final PersistentAdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding ) {
		final @Nullable QueryFilter clientFilter = getClientFilter(options);
		if (options == null || !options.isServerSideLibraryEnabled()) {
			final Map<Short, RowMergingDataAdapter> mergingAdapters = getMergingAdapters(adapterStore);

			if (!mergingAdapters.isEmpty()) {
				return new GeoWaveRowIteratorTransformer<T>() {

					@SuppressWarnings({
						"rawtypes",
						"unchecked"
					})
					@Override
					public Iterator<T> apply(
							Iterator<GeoWaveRow> input ) {
						return new MergingEntryIterator(
								adapterStore,
								index,
								input,
								clientFilter,
								scanCallback,
								mergingAdapters,
								maxResolutionSubsamplingPerDimension);
					}
				};
			}
		}

		return new GeoWaveRowIteratorTransformer<T>() {

			@SuppressWarnings({
				"rawtypes",
				"unchecked"
			})
			@Override
			public Iterator<T> apply(
					Iterator<GeoWaveRow> input ) {
				return new NativeEntryIteratorWrapper(
						adapterStore,
						index,
						input,
						clientFilter,
						scanCallback,
						getFieldBitmask(),
						// Don't do client side subsampling if server side is
						// enabled.
						(options != null && options.isServerSideLibraryEnabled()) ? null
								: maxResolutionSubsamplingPerDimension,
						decodePersistenceEncoding);
			}

		};
	}

	@Override
	protected QueryFilter getClientFilter(
			final DataStoreOptions options ) {
		final List<QueryFilter> internalClientFilters = getClientFiltersList(options);
		return internalClientFilters.isEmpty() ? null : internalClientFilters.size() == 1 ? internalClientFilters
				.get(0) : new FilterList<QueryFilter>(
				internalClientFilters);
	}

	protected List<QueryFilter> getClientFiltersList(
			final DataStoreOptions options ) {
		return clientFilters;
	}
}
