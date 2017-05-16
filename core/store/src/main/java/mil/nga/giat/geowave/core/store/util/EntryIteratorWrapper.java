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

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
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
	protected final Iterator scannerIt;
	protected final QueryFilter clientFilter;
	protected final ScanCallback<T> scanCallback;
	protected final boolean wholeRowEncoding;

	protected T nextValue;

	public EntryIteratorWrapper(
			final boolean wholeRowEncoding,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback ) {
		this.adapterStore = adapterStore;
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.scanCallback = scanCallback;
		this.wholeRowEncoding = wholeRowEncoding;
	}

	private void findNext() {
		while ((nextValue == null) && hasNextScannedResult()) {
			final Object row = getNextEncodedResult();
			final T decodedValue = decodeRow(
					row,
					clientFilter,
					index,
					wholeRowEncoding);
			if (decodedValue != null) {
				nextValue = decodedValue;
				return;
			}
		}
	}

	protected boolean hasNextScannedResult() {
		return scannerIt.hasNext();
	}

	protected Object getNextEncodedResult() {
		return scannerIt.next();
	}

	protected abstract T decodeRow(
			final Object row,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			boolean wholeRowEncoding );

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
