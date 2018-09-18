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
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.query.filter.DistributableQueryFilter;

public class ReaderParams<T> extends
		BaseReaderParams<T>
{
	private final boolean isServersideAggregation;
	private final boolean isClientsideRowMerging;
	private final QueryRanges queryRanges;
	private final DistributableQueryFilter filter;
	private final List<MultiDimensionalCoordinateRangesArray> coordinateRanges;
	private final List<MultiDimensionalNumericData> constraints;

	public ReaderParams(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final List<Short> adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, InternalDataAdapter<?>> fieldSubsets,
			final boolean isMixedVisibility,
			final boolean isAuthorizationsLimiting,
			final boolean isServersideAggregation,
			final boolean isClientsideRowMerging,
			final QueryRanges queryRanges,
			final DistributableQueryFilter filter,
			final Integer limit,
			final Integer maxRangeDecomposition,
			final List<MultiDimensionalCoordinateRangesArray> coordinateRanges,
			final List<MultiDimensionalNumericData> constraints,
			final GeoWaveRowIteratorTransformer<T> rowTransformer,
			final String... additionalAuthorizations ) {
		super(
				index,
				adapterStore,
				adapterIds,
				maxResolutionSubsamplingPerDimension,
				aggregation,
				fieldSubsets,
				isMixedVisibility,
				isAuthorizationsLimiting,
				limit,
				maxRangeDecomposition,
				rowTransformer,
				additionalAuthorizations);
		this.isServersideAggregation = isServersideAggregation;
		this.isClientsideRowMerging = isClientsideRowMerging;
		this.queryRanges = queryRanges;
		this.filter = filter;
		this.coordinateRanges = coordinateRanges;
		this.constraints = constraints;
	}

	public List<MultiDimensionalCoordinateRangesArray> getCoordinateRanges() {
		return coordinateRanges;
	}

	public List<MultiDimensionalNumericData> getConstraints() {
		return constraints;
	}

	public boolean isClientsideRowMerging() {
		return isClientsideRowMerging;
	}

	public boolean isServersideAggregation() {
		return isServersideAggregation;
	}

	public QueryRanges getQueryRanges() {
		return queryRanges;
	}

	public DistributableQueryFilter getFilter() {
		return filter;
	}
}
