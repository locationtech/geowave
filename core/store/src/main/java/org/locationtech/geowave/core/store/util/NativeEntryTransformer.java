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
package org.locationtech.geowave.core.store.util;

import java.util.Iterator;

import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class NativeEntryTransformer<T> implements
		GeoWaveRowIteratorTransformer<T>
{
	private final PersistentAdapterStore adapterStore;
	private final Index index;
	private final QueryFilter clientFilter;
	private final ScanCallback<T, ? extends GeoWaveRow> scanCallback;
	private final byte[] fieldSubsetBitmask;
	private final double[] maxResolutionSubsamplingPerDimension;
	private final boolean decodePersistenceEncoding;

	public NativeEntryTransformer(
			final PersistentAdapterStore adapterStore,
			final Index index,
			final QueryFilter clientFilter,
			final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
			final byte[] fieldSubsetBitmask,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.clientFilter = clientFilter;
		this.scanCallback = scanCallback;
		this.fieldSubsetBitmask = fieldSubsetBitmask;
		this.decodePersistenceEncoding = decodePersistenceEncoding;
		this.maxResolutionSubsamplingPerDimension = maxResolutionSubsamplingPerDimension;
	}

	@Override
	public Iterator<T> apply(
			Iterator<GeoWaveRow> rowIter ) {
		return new NativeEntryIteratorWrapper<T>(
				adapterStore,
				index,
				rowIter,
				clientFilter,
				scanCallback,
				fieldSubsetBitmask,
				maxResolutionSubsamplingPerDimension,
				decodePersistenceEncoding);
	}
}
