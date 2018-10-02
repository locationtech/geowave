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

import org.apache.log4j.Logger;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.callback.ScanCallbackList;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.Reader;
import org.locationtech.geowave.core.store.operations.ReaderClosableWrapper;
import org.locationtech.geowave.core.store.util.NativeEntryTransformer;

/**
 * Represents a query operation by an Accumulo row. This abstraction is
 * re-usable for both exact row ID queries and row prefix queries.
 *
 */
abstract class AbstractBaseRowQuery<T> extends
		BaseQuery
{
	private static final Logger LOGGER = Logger.getLogger(AbstractBaseRowQuery.class);

	public AbstractBaseRowQuery(
			final PrimaryIndex index,
			final String[] authorizations,
			final ScanCallback<T, ?> scanCallback,
			final DifferingFieldVisibilityEntryCount differingVisibilityCounts,
			final FieldVisibilityCount visibilityCounts ) {
		super(
				index,
				scanCallback,
				differingVisibilityCounts,
				visibilityCounts,
				authorizations);
	}

	public CloseableIterator<T> query(
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final double[] maxResolutionSubsamplingPerDimension,
			final PersistentAdapterStore adapterStore,
			final Integer limit,
			final Integer queryMaxRangeDecomposition,
			boolean delete ) {
		Reader<T> reader = getReader(
				operations,
				options,
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit,
				queryMaxRangeDecomposition,
				new NativeEntryTransformer<T>(
						adapterStore,
						index,
						getClientFilter(options),
						(ScanCallback<T, ?>) scanCallback,
						getFieldBitmask(),
						maxResolutionSubsamplingPerDimension,
						!isCommonIndexAggregation()),
				delete);
		return new CloseableIteratorWrapper<T>(
				new ReaderClosableWrapper(
						reader),
				reader);
	}
}
