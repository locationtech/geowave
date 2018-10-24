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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.FilterByTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

public class BaseQueryOptions
{
	private static Logger LOGGER = LoggerFactory.getLogger(BaseQueryOptions.class);
	private static ScanCallback<Object, GeoWaveRow> DEFAULT_CALLBACK = new ScanCallback<Object, GeoWaveRow>() {
		@Override
		public void entryScanned(
				final Object entry,
				final GeoWaveRow row ) {}
	};

	@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = {
		"SE_TRANSIENT_FIELD_NOT_RESTORED"
	})
	private Collection<InternalDataAdapter<?>> adapters = null;
	private short[] adapterIds = null;
	private String indexName = null;
	private transient Index index = null;
	private Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregationAdapterPair;
	private Integer limit = -1;
	private Integer maxRangeDecomposition = null;
	private double[] maxResolutionSubsamplingPerDimension = null;
	private double[] targetResolutionPerDimensionForHierarchicalIndex = null;
	private transient ScanCallback<?, ?> scanCallback = DEFAULT_CALLBACK;
	private String[] authorizations = new String[0];
	private Pair<String[], InternalDataAdapter<?>> fieldIdsAdapterPair;
	private boolean nullId = false;

	public BaseQueryOptions(
			final Query<?> query,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore ) {
		this(
				query,
				adapterStore,
				internalAdapterStore,
				null);
	}

	public BaseQueryOptions(
			final AggregationQuery<?, ?, ?> query,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore ) {
		this(
				query.getCommonQueryOptions(),
				query.getDataTypeQueryOptions(),
				query.getIndexQueryOptions(),
				adapterStore,
				internalAdapterStore,
				null);
	}

	public BaseQueryOptions(
			final Query<?> query,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final ScanCallback<?, ?> scanCallback ) {
		this(
				query.getCommonQueryOptions(),
				query.getDataTypeQueryOptions(),
				query.getIndexQueryOptions(),
				adapterStore,
				internalAdapterStore,
				scanCallback);
	}

	public BaseQueryOptions(
			final CommonQueryOptions commonOptions,
			final DataTypeQueryOptions<?> typeOptions,
			final IndexQueryOptions indexOptions,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore ) {
		this(
				commonOptions,
				typeOptions,
				indexOptions,
				adapterStore,
				internalAdapterStore,
				null);
	}

	public BaseQueryOptions(
			final CommonQueryOptions commonOptions,
			final DataTypeQueryOptions<?> typeOptions,
			final IndexQueryOptions indexOptions,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final ScanCallback<?, ?> scanCallback ) {
		if (scanCallback != null) {
			this.scanCallback = scanCallback;
		}
		indexName = indexOptions.getIndexName();
		limit = commonOptions.getLimit();
		maxRangeDecomposition = (Integer) commonOptions.getHints().get(
				DataStoreUtils.MAX_RANGE_DECOMPOSITION);
		maxResolutionSubsamplingPerDimension = (double[]) commonOptions.getHints().get(
				DataStoreUtils.MAX_RESOLUTION_SUBSAMPLING_PER_DIMENSION);
		targetResolutionPerDimensionForHierarchicalIndex = (double[]) commonOptions.getHints().get(
				DataStoreUtils.TARGET_RESOLUTION_PER_DIMENSION_FOR_HIERARCHICAL_INDEX);
		authorizations = commonOptions.getAuthorizations();

		if ((typeOptions instanceof AggregateTypeQueryOptions)
				&& (((AggregateTypeQueryOptions) typeOptions).getAggregation() != null)) {
			// TODO issue #1439 addresses being able to handle multiple types
			// within aa single aggregation
			// it seems that the best approach would check if its a
			// commonindexaggregation and then it can be done with a single
			// query with simply adapter IDs rather than even needing adapters,
			// but if its not commonindexaggregation it would require multiple
			// adapters either in the context of a single query or multiple
			// queries, one per adapter and then aggregating the final result
			// for now let's just assume a single type name and get the adapter,
			// rather than just type name (which type name would be sufficient
			// for commonindexaggregation)
			if (((AggregateTypeQueryOptions) typeOptions).getTypeNames().length == 1) {
				final String typeName = ((AggregateTypeQueryOptions) typeOptions).getTypeNames()[0];
				final Short adapterId = internalAdapterStore.getAdapterId(typeName);
				if (adapterId != null) {
					final DataTypeAdapter<?> adapter = adapterStore.getAdapter(adapterId);
					aggregationAdapterPair = new ImmutablePair<>(
							new InternalDataAdapterWrapper<>(
									adapter,
									adapterId),
							((AggregateTypeQueryOptions) typeOptions).getAggregation());
				}
				else {
					throw new IllegalArgumentException(
							"Type name " + typeName + " does not exist");
				}
			}
			else {
				// TODO GEOWAVE issue #1439 should tackle this case
				throw new IllegalArgumentException(
						"Single type name supported currently");
			}
		}

		else if ((typeOptions instanceof FilterByTypeQueryOptions)
				&& (((FilterByTypeQueryOptions) typeOptions).getFieldNames() != null)
				&& (((FilterByTypeQueryOptions) typeOptions).getFieldNames().length > 0)
				&& (((FilterByTypeQueryOptions) typeOptions).getTypeNames().length > 0)) {
			// filter by type for field subsetting only allows a single type
			// name
			final String typeName = ((FilterByTypeQueryOptions) typeOptions).getTypeNames()[0];
			if (typeName != null) {
				final Short adapterId = internalAdapterStore.getAdapterId(typeName);
				if (adapterId != null) {
					final DataTypeAdapter<?> adapter = adapterStore.getAdapter(adapterId);
					fieldIdsAdapterPair = new ImmutablePair<>(
							((FilterByTypeQueryOptions) typeOptions).getFieldNames(),
							new InternalDataAdapterWrapper<>(
									adapter,
									adapterId));
				}
				else {
					throw new IllegalArgumentException(
							"Type name " + typeName + " does not exist");
				}
			}
			else {
				throw new IllegalArgumentException(
						"Type name cannot be null for field subsetting");
			}
		}

		if ((typeOptions != null) && (typeOptions.getTypeNames() != null) && (typeOptions.getTypeNames().length > 0)) {
			adapterIds = ArrayUtils.toPrimitive(Collections2.filter(
					Lists.transform(
							Arrays.asList(typeOptions.getTypeNames()),
							new Function<String, Short>() {
								@Override
								public Short apply(
										final String input ) {
									return internalAdapterStore.getAdapterId(input);
								}
							}),
					new Predicate<Short>() {
						@Override
						public boolean apply(
								final Short input ) {
							if (input == null) {
								nullId = true;
								return false;
							}
							return true;
						}
					}).toArray(
					new Short[0]));
		}
	}

	public boolean isAllAdapters() {
		// TODO what about field ID subsetting and aggregation which implicitly
		// filters by adapter
		return ((adapterIds == null) || (adapterIds.length == 0));
	}

	/**
	 * Return the set of adapter/index associations. If the adapters are not
	 * provided, then look up all of them. If the index is not provided, then
	 * look up all of them.
	 *
	 * DataStores are responsible for selecting a single adapter/index per
	 * query. For deletions, the Data Stores are interested in all the
	 * associations.
	 *
	 * @param adapterStore
	 * @param
	 * @param indexStore
	 * @return
	 * @throws IOException
	 */

	public List<Pair<Index, List<InternalDataAdapter<?>>>> getIndicesForAdapters(
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		return BaseDataStoreUtils.combineByIndex(compileIndicesForAdapters(
				adapterStore,
				adapterIndexMappingStore,
				indexStore));
	}

	public InternalDataAdapter<?>[] getAdaptersArray(
			final PersistentAdapterStore adapterStore )
			throws IOException {
		if ((adapterIds != null) && (adapterIds.length != 0)) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<>();
				for (final Short id : adapterIds) {
					if (id == null) {
						nullId = true;
						continue;
					}
					final InternalDataAdapter<?> adapter = adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
					else {
						nullId = true;
					}
				}
			}
			return adapters.toArray(new InternalDataAdapter[adapters.size()]);
		}
		if (nullId) {
			return new InternalDataAdapter[] {};
		}
		final List<InternalDataAdapter<?>> list = new ArrayList<>();
		if (adapterStore != null) {
			final CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters();
			if (it != null) {
				while (it.hasNext()) {
					list.add(it.next());
				}
				it.close();
			}
		}
		return list.toArray(new InternalDataAdapter[list.size()]);
	}

	public void setAdapterId(
			final Short adapterId ) {
		if (adapterId != null) {
			adapterIds = new short[] {
				adapterId
			};
		}
	}

	public short[] getAdapterIds() {
		return adapterIds;
	}

	private List<Pair<Index, InternalDataAdapter<?>>> compileIndicesForAdapters(
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		if ((adapterIds != null) && (adapterIds.length != 0)) {
			if ((adapters == null) || adapters.isEmpty()) {
				adapters = new ArrayList<>();
				for (final short id : adapterIds) {
					final InternalDataAdapter<?> adapter = adapterStore.getAdapter(id);
					if (adapter != null) {
						adapters.add(adapter);
					}
				}
			}
		}
		else if (!nullId && ((adapters == null) || adapters.isEmpty())) {
			adapters = new ArrayList<>();
			try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
				while (it.hasNext()) {
					adapters.add(it.next());
				}
			}
		}
		else if (adapters == null) {
			adapters = Collections.emptyList();
		}
		final List<Pair<Index, InternalDataAdapter<?>>> result = new ArrayList<>();
		for (final InternalDataAdapter<?> adapter : adapters) {
			final AdapterToIndexMapping indices = adapterIndexMappingStore.getIndicesForAdapter(adapter.getAdapterId());
			if (index != null) {
				result.add(Pair.of(
						index,
						adapter));
			}
			else if ((indexName != null) && indices.contains(indexName)) {
				if (index == null) {
					index = indexStore.getIndex(indexName);
					result.add(Pair.of(
							index,
							adapter));
				}
			}
			else if (indices.isNotEmpty()) {
				for (final String name : indices.getIndexNames()) {
					final Index pIndex = indexStore.getIndex(name);
					// this could happen if persistent was turned off
					if (pIndex != null) {
						result.add(Pair.of(
								pIndex,
								adapter));
					}
				}
			}
		}
		return result;
	}

	public ScanCallback<?, ?> getScanCallback() {
		return scanCallback == null ? DEFAULT_CALLBACK : scanCallback;
	}

	/**
	 * @param scanCallback
	 *            a function called for each item discovered per the query
	 *            constraints
	 */
	public void setScanCallback(
			final ScanCallback<?, ?> scanCallback ) {
		this.scanCallback = scanCallback;
	}

	/**
	 *
	 * @return the max range decomposition to use when computing query ranges
	 */
	public Integer getMaxRangeDecomposition() {
		return maxRangeDecomposition;
	}

	/**
	 * a value of null indicates to use the data store configured default
	 *
	 * @param maxRangeDecomposition
	 */
	public void setMaxRangeDecomposition(
			final Integer maxRangeDecomposition ) {
		this.maxRangeDecomposition = maxRangeDecomposition;
	}

	/**
	 *
	 * @return Limit the number of data items to return
	 */
	public Integer getLimit() {
		return limit;
	}

	/**
	 * a value <= 0 or null indicates no limits
	 *
	 * @param limit
	 */
	public void setLimit(
			Integer limit ) {
		if ((limit == null) || (limit == 0)) {
			limit = -1;
		}
		this.limit = limit;
	}

	/**
	 *
	 * @return authorizations to apply to the query in addition to the
	 *         authorizations assigned to the data store as a whole.
	 */
	public String[] getAuthorizations() {
		return authorizations == null ? new String[0] : authorizations;
	}

	public void setAuthorizations(
			final String[] authorizations ) {
		this.authorizations = authorizations;
	}

	public double[] getTargetResolutionPerDimensionForHierarchicalIndex() {
		return targetResolutionPerDimensionForHierarchicalIndex;
	}

	public void setTargetResolutionPerDimensionForHierarchicalIndex(
			final double[] targetResolutionPerDimensionForHierarchicalIndex ) {
		this.targetResolutionPerDimensionForHierarchicalIndex = targetResolutionPerDimensionForHierarchicalIndex;
	}

	public void setMaxResolutionSubsamplingPerDimension(
			final double[] maxResolutionSubsamplingPerDimension ) {
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregationAdapterPair;
	}

	public void setAggregation(
			final Aggregation<?, ?, ?> aggregation,
			final InternalDataAdapter<?> adapter ) {
		aggregationAdapterPair = new ImmutablePair<>(
				adapter,
				aggregation);
	}

	/**
	 * Return a set list adapter/index associations. If the adapters are not
	 * provided, then look up all of them. If the index is not provided, then
	 * look up all of them. The full set of adapter/index associations is
	 * reduced so that a single index is queried per adapter and the number
	 * indices queried is minimized.
	 *
	 * DataStores are responsible for selecting a single adapter/index per
	 * query. For deletions, the Data Stores are interested in all the
	 * associations.
	 *
	 * @param adapterStore
	 * @param adapterIndexMappingStore
	 * @param indexStore
	 * @return
	 * @throws IOException
	 */
	public List<Pair<Index, List<InternalDataAdapter<?>>>> getAdaptersWithMinimalSetOfIndices(
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final IndexStore indexStore )
			throws IOException {
		// TODO this probably doesn't have to use PrimaryIndex and should be
		// sufficient to use index IDs
		return BaseDataStoreUtils.reduceIndicesAndGroupByIndex(compileIndicesForAdapters(
				adapterStore,
				adapterIndexMappingStore,
				indexStore));
	}

	/**
	 *
	 * @return a paring of fieldIds and their associated data adapter >>>>>>>
	 *         wip: bitmask approach
	 */
	public Pair<String[], InternalDataAdapter<?>> getFieldIdsAdapterPair() {
		return fieldIdsAdapterPair;
	}

	public short[] getValidAdapterIds(
			final InternalAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore )
			throws IOException {
		// Grab the list of adapter ids, either from the query (if included),
		// Or the whole list from the adapter store...
		final short[] adapterIds = getAdapterIds(adapterStore);

		// Then for each adapter, verify that it exists in the index-adapter
		// mapping
		final List<Short> validIds = new ArrayList<>();
		for (final short adapterId : adapterIds) {
			final AdapterToIndexMapping mapping = adapterIndexMappingStore.getIndicesForAdapter(adapterId);
			if (mapping.contains(indexName)) {
				validIds.add(adapterId);
			}
		}

		return ArrayUtils.toPrimitive(validIds.toArray(new Short[0]));
	}

	public short[] getAdapterIds(
			final InternalAdapterStore adapterStore ) {
		if ((adapterIds == null) || (adapterIds.length == 0)) {
			return adapterStore.getAdapterIds();
		}
		else {
			return adapterIds;
		}
	}
}
