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

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.IndexUtils;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.AdapterException;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.filter.QueryFilter;
import org.locationtech.geowave.core.store.index.PrimaryIndex;

public class NativeEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final byte[] fieldSubsetBitmask;
	private final boolean decodePersistenceEncoding;
	private Integer bitPosition = null;
	private ByteArrayId skipUntilRow;
	private boolean reachedEnd = false;
	private boolean adapterValid = true;

	public NativeEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<GeoWaveRow> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, ? extends GeoWaveRow> scanCallback,
			final byte[] fieldSubsetBitmask,
			final double[] maxResolutionSubsamplingPerDimension,
			final boolean decodePersistenceEncoding ) {
		super(
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback);
		this.fieldSubsetBitmask = fieldSubsetBitmask;
		this.decodePersistenceEncoding = decodePersistenceEncoding;

		initializeBitPosition(maxResolutionSubsamplingPerDimension);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected T decodeRow(
			final GeoWaveRow row,
			final QueryFilter clientFilter,
			final PrimaryIndex index ) {
		Object decodedRow = null;
		if (adapterValid && (bitPosition == null || passesSkipFilter(row))) {
			try {
				decodedRow = BaseDataStoreUtils.decodeRow(
						row,
						clientFilter,
						null,
						adapterStore,
						index,
						scanCallback,
						fieldSubsetBitmask,
						decodePersistenceEncoding);

				if (decodedRow != null) {
					incrementSkipRow(row);
				}
			}
			catch (AdapterException e) {
				adapterValid = false;
				// Attempting to decode future rows with the same adapter is
				// pointless.
			}
		}
		return (T) decodedRow;
	}

	private boolean passesSkipFilter(
			final GeoWaveRow row ) {
		if ((reachedEnd == true) || ((skipUntilRow != null) && (skipUntilRow.compareTo(new ByteArrayId(
				GeoWaveKey.getCompositeId(row))) > 0))) {
			return false;
		}

		return true;
	}

	private void incrementSkipRow(
			final GeoWaveRow row ) {
		if (bitPosition != null) {
			final byte[] nextRow = IndexUtils.getNextRowForSkip(
					GeoWaveKey.getCompositeId(row),
					bitPosition);

			if (nextRow == null) {
				reachedEnd = true;
			}
			else {
				skipUntilRow = new ByteArrayId(
						nextRow);
			}
		}
	}

	private void initializeBitPosition(
			final double[] maxResolutionSubsamplingPerDimension ) {
		if ((maxResolutionSubsamplingPerDimension != null) && (maxResolutionSubsamplingPerDimension.length > 0)) {
			bitPosition = IndexUtils.getBitPositionFromSubsamplingArray(
					index.getIndexStrategy(),
					maxResolutionSubsamplingPerDimension);
		}
	}
}
