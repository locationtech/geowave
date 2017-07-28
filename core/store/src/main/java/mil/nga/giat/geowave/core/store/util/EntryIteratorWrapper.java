/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This is used internally to translate DataStore rows into native objects
 * (using the appropriate data adapter). It also performs any client-side
 * filtering. It will peek at the next entry in the wrapped iterator to always
 * maintain a reference to the next value.
 *
 * @param <T>
 *            The type for the entry
 */
public abstract class EntryIteratorWrapper<T> implements
		Iterator<T>
{
	protected final AdapterStore adapterStore;
	protected final PrimaryIndex index;
	protected final Iterator<GeoWaveRow> scannerIt;
	protected final QueryFilter clientFilter;
	protected final ScanCallback<T, ? extends GeoWaveRow> scanCallback;

	protected T nextValue;

	public EntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<GeoWaveRow> scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T, ? extends GeoWaveRow> scanCallback ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.scanCallback = scanCallback;
	}

	private void findNext() {
		while ((nextValue == null) && hasNextScannedResult()) {
			final GeoWaveRow row = getNextEncodedResult();
			final T decodedValue = decodeRow(
					row,
					clientFilter,
					index);
			if (decodedValue != null) {
				nextValue = decodedValue;
				return;
			}
		}
	}

	protected boolean hasNextScannedResult() {
		return scannerIt.hasNext();
	}

	protected GeoWaveRow getNextEncodedResult() {
		return scannerIt.next();
	}

	protected abstract T decodeRow(
			final GeoWaveRow row,
			final QueryFilter clientFilter,
			final PrimaryIndex index );

	@Override
	public boolean hasNext() {
		findNext();
		return nextValue != null;
	}

	@Override
	public T next()
			throws NoSuchElementException {
		if (nextValue == null) {
			findNext();
		}
		final T previousNext = nextValue;
		if (nextValue == null) {
			throw new NoSuchElementException();
		}
		nextValue = null;
		return previousNext;
	}

	@Override
	public void remove() {
		scannerIt.remove();
	}

}
