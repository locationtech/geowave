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
package org.locationtech.geowave.core.store.operations;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

abstract public class BaseReaderParams<T>
{

	private final Index index;
	private final PersistentAdapterStore adapterStore;
	private final InternalAdapterStore internalAdapterStore;
	private final short[] adapterIds;
	private final double[] maxResolutionSubsamplingPerDimension;
	private final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation;
	private final Pair<String[], InternalDataAdapter<?>> fieldSubsets;
	private final boolean isMixedVisibility;
	private final boolean isAuthorizationsLimiting;
	private final Integer limit;
	private final Integer maxRangeDecomposition;
	private final GeoWaveRowIteratorTransformer<T> rowTransformer;
	private final String[] additionalAuthorizations;

	public BaseReaderParams(
			final Index index,
			final PersistentAdapterStore adapterStore,
			InternalAdapterStore internalAdapterStore,
			final short[] adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
			final boolean isMixedVisibility,
			final boolean isAuthorizationsLimiting,
			final Integer limit,
			final Integer maxRangeDecomposition,
			final GeoWaveRowIteratorTransformer<T> rowTransformer,
			final String[] additionalAuthorizations ) {
		this.index = index;
		this.adapterStore = adapterStore;
		this.internalAdapterStore = internalAdapterStore;
		this.adapterIds = adapterIds;
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
		this.aggregation = aggregation;
		this.fieldSubsets = fieldSubsets;
		this.isMixedVisibility = isMixedVisibility;
		this.isAuthorizationsLimiting = isAuthorizationsLimiting;
		this.limit = limit;
		this.maxRangeDecomposition = maxRangeDecomposition;
		this.rowTransformer = rowTransformer;
		this.additionalAuthorizations = additionalAuthorizations;
	}

	public Index getIndex() {
		return index;
	}

	public PersistentAdapterStore getAdapterStore() {
		return adapterStore;
	}

	public InternalAdapterStore getInternalAdapterStore() {
		return internalAdapterStore;
	}

	public short[] getAdapterIds() {
		return adapterIds;
	}

	public double[] getMaxResolutionSubsamplingPerDimension() {
		return maxResolutionSubsamplingPerDimension;
	}

	public Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return aggregation;
	}

	public Pair<String[], InternalDataAdapter<?>> getFieldSubsets() {
		return fieldSubsets;
	}

	public boolean isAuthorizationsLimiting() {
		return isAuthorizationsLimiting;
	}

	public boolean isMixedVisibility() {
		return isMixedVisibility;
	}

	public boolean isAggregation() {
		return ((aggregation != null) && (aggregation.getRight() != null));
	}

	public Integer getLimit() {
		return limit;
	}

	public Integer getMaxRangeDecomposition() {
		return maxRangeDecomposition;
	}

	public String[] getAdditionalAuthorizations() {
		return additionalAuthorizations;
	}

	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		return null;
	}

	public List<MultiDimensionalNumericData> getConstraints() {
		return null;
	}

	public QueryFilter getFilter() {
		return null;
	}

	public boolean isServersideAggregation() {
		return false;
	}

	public GeoWaveRowIteratorTransformer<T> getRowTransformer() {
		return rowTransformer;
	}
}
