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

import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

class IntermediaryReadEntryInfo<T>
{
	private final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
	private final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
	private final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();
	private final boolean decodeRow;
	private final Index index;

	private InternalDataAdapter<T> dataAdapter;
	private boolean adapterVerified;

	public IntermediaryReadEntryInfo(
			final Index index,
			final boolean decodeRow ) {
		this.index = index;
		this.decodeRow = decodeRow;
	}

	public Index getIndex() {
		return index;
	}

	public boolean isDecodeRow() {
		return decodeRow;
	}

	// Adapter is set either by the user or from the data
	// If null, expect it from data, so no verify needed
	public boolean setDataAdapter(
			final InternalDataAdapter<T> dataAdapter,
			final boolean fromData ) {
		this.dataAdapter = dataAdapter;
		this.adapterVerified = fromData ? true : (dataAdapter == null);
		return hasDataAdapter();
	}

	public boolean verifyAdapter(
			final short internalAdapterId ) {
		if ((this.dataAdapter == null) || (internalAdapterId == 0)) {
			return false;
		}

		this.adapterVerified = (internalAdapterId == dataAdapter.getAdapterId()) ? true : false;

		return this.adapterVerified;
	}

	public boolean setOrRetrieveAdapter(
			final InternalDataAdapter<T> adapter,
			final short internalAdapterId,
			final PersistentAdapterStore adapterStore ) {
		// Verify the current data adapter
		if (setDataAdapter(
				adapter,
				false)) {
			return true;
		}

		// Can't retrieve an adapter without the store
		if (adapterStore == null) {
			return false;
		}

		// Try to retrieve the adapter from the store
		if (setDataAdapter(
				(InternalDataAdapter<T>) adapterStore.getAdapter(internalAdapterId),
				true)) {
			return true;
		}

		// No adapter set or retrieved
		return false;
	}

	public boolean isAdapterVerified() {
		return this.adapterVerified;
	}

	public boolean hasDataAdapter() {
		return this.dataAdapter != null;
	}

	public InternalDataAdapter<T> getDataAdapter() {
		return dataAdapter;
	}

	public String getTypeName() {
		if (dataAdapter != null) {
			return dataAdapter.getTypeName();
		}

		return null;
	}

	public PersistentDataset<CommonIndexValue> getIndexData() {
		return indexData;
	}

	public PersistentDataset<Object> getExtendedData() {
		return extendedData;
	}

	public PersistentDataset<byte[]> getUnknownData() {
		return unknownData;
	}
}
