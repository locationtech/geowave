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
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.callback.ScanCallbackList;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.filter.DistributableQueryFilter;
import org.locationtech.geowave.core.store.filter.QueryFilter;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.Reader;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.query.aggregate.Aggregation;

/**
 * This class is used internally to perform query operations against a base data
 * store. The query is defined by the set of parameters passed into the
 * constructor.
 */
abstract class BaseQuery
{
	private final static Logger LOGGER = Logger.getLogger(BaseQuery.class);

	protected List<Short> adapterIds;
	protected final PrimaryIndex index;
	protected final Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair;
	protected final DifferingFieldVisibilityEntryCount differingVisibilityCounts;
	protected final FieldVisibilityCount visibilityCounts;
	protected final String[] authorizations;
	protected final ScanCallbackList<?, ?> scanCallback;

	public BaseQuery(
			final PrimaryIndex index,
			final ScanCallback<?, ?> scanCallback,
			final DifferingFieldVisibilityEntryCount differingVisibilityCounts,
			FieldVisibilityCount visibilityCounts,
			final String... authorizations ) {
		this(
				null,
				index,
				null,
				scanCallback,
				differingVisibilityCounts,
				visibilityCounts,
				authorizations);
	}

	public BaseQuery(
			final List<Short> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, InternalDataAdapter<?>> fieldIdsAdapterPair,
			final ScanCallback<?, ?> scanCallback,
			final DifferingFieldVisibilityEntryCount differingVisibilityCounts,
			FieldVisibilityCount visibilityCounts,
			final String... authorizations ) {
		this.adapterIds = adapterIds;
		this.index = index;
		this.fieldIdsAdapterPair = fieldIdsAdapterPair;
		this.differingVisibilityCounts = differingVisibilityCounts;
		this.visibilityCounts = visibilityCounts;
		this.authorizations = authorizations;

		List<ScanCallback<?, ?>> callbacks = new ArrayList<>();
		if (scanCallback != null) {
			callbacks.add(scanCallback);
		}
		this.scanCallback = (ScanCallbackList) new ScanCallbackList(
				callbacks);
	}

	protected <C> Reader<C> getReader(
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final PersistentAdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit,
			final Integer queryMaxRangeDecomposition,
			final GeoWaveRowIteratorTransformer<C> rowTransformer,
			boolean delete ) {
		final int maxRangeDecomposition;
		if (queryMaxRangeDecomposition != null) {
			maxRangeDecomposition = queryMaxRangeDecomposition;
		}
		else {
			maxRangeDecomposition = isAggregation() ? options.getAggregationMaxRangeDecomposition() : options
					.getMaxRangeDecomposition();
		}
		ReaderParams<C> readerParams = new ReaderParams<C>(
				index,
				adapterStore,
				adapterIds,
				maxResolutionSubsamplingPerDimension,
				getAggregation(),
				getFieldSubsets(),
				isMixedVisibilityRows(),
				isAuthorizationsLimiting(),
				isServerSideAggregation(options),
				isRowMerging(adapterStore),
				getRanges(maxRangeDecomposition),
				getServerFilter(options),
				limit,
				maxRangeDecomposition,
				getCoordinateRanges(),
				getConstraints(),
				rowTransformer,
				getAdditionalAuthorizations());
		if (delete) {
			scanCallback.waitUntilCallbackAdded();
			Deleter<C> deleter = operations.createDeleter(readerParams);
			scanCallback.addScanCallback((ScanCallback) deleter);
			return deleter;
		}
		return operations.createReader(readerParams);
	}

	public boolean isRowMerging(
			PersistentAdapterStore adapterStore ) {
		if (adapterIds != null) {
			for (short adapterId : adapterIds) {
				if (adapterStore.getAdapter(
						adapterId).getAdapter() instanceof RowMergingDataAdapter) {
					return true;
				}
			}
		}
		else {
			try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
				while (it.hasNext()) {
					if (it.next().getAdapter() instanceof RowMergingDataAdapter) {
						return true;
					}
				}
			}
			catch (IOException e) {
				LOGGER.error(
						"Unable to close adapter store iterator",
						e);
			}
		}
		return false;
	}

	public boolean isServerSideAggregation(
			final DataStoreOptions options ) {
		return ((options != null) && options.isServerSideLibraryEnabled() && isAggregation());
	}

	public boolean isAggregation() {
		final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation = getAggregation();
		return ((aggregation != null) && (aggregation.getRight() != null));
	}

	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		return null;
	}

	public List<MultiDimensionalNumericData> getConstraints() {
		return null;
	}

	abstract protected QueryRanges getRanges(
			int maxRangeDecomposition );

	protected Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return null;
	}

	protected Pair<List<String>, InternalDataAdapter<?>> getFieldSubsets() {
		return fieldIdsAdapterPair;
	}

	protected byte[] getFieldBitmask() {
		if (fieldIdsAdapterPair != null && fieldIdsAdapterPair.getLeft() != null) {
			return BitmaskUtils.generateFieldSubsetBitmask(
					index.getIndexModel(),
					ByteArrayId.transformStringList(fieldIdsAdapterPair.getLeft()),
					fieldIdsAdapterPair.getRight());
		}

		return null;
	}

	protected boolean isAuthorizationsLimiting() {
		return (visibilityCounts == null) || visibilityCounts.isAuthorizationsLimiting(authorizations);
	}

	protected boolean isMixedVisibilityRows() {
		return (differingVisibilityCounts == null) || differingVisibilityCounts.isAnyEntryDifferingFieldVisiblity();
	}

	public String[] getAdditionalAuthorizations() {
		return authorizations;
	}

	public DistributableQueryFilter getServerFilter(
			final DataStoreOptions options ) {
		return null;
	}

	protected QueryFilter getClientFilter(
			final DataStoreOptions options ) {
		return null;
	}

	protected boolean isCommonIndexAggregation() {
		return false;
	}
}
