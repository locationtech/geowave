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
package org.locationtech.geowave.mapreduce.splits;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.operations.BaseReaderParams;

public class RecordReaderParams<T> extends
		BaseReaderParams<T>
{
	private final GeoWaveRowRange rowRange;

	public RecordReaderParams(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final short[] adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<String[], InternalDataAdapter<?>> fieldSubsets,
			final boolean isMixedVisibility,
			final boolean isAuthorizationsLimiting,
			final GeoWaveRowRange rowRange,
			final Integer limit,
			final Integer maxRangeDecomposition,
			final GeoWaveRowIteratorTransformer<T> rowTransformer,
			final String... additionalAuthorizations ) {
		super(
				index,
				adapterStore,
				internalAdapterStore,
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
		this.rowRange = rowRange;
	}

	public GeoWaveRowRange getRowRange() {
		return rowRange;
	}
}
